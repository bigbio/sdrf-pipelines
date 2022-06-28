/**
 * @typedef {import('vfile').VFile} VFile
 * @typedef {import('vfile-message').VFileMessage} VFileMessage
 * @typedef {import('vfile-statistics').Statistics} Statistics
 *
 * @typedef Options
 * @property {boolean} [color]
 * @property {boolean} [silent=false]
 * @property {boolean} [quiet=false]
 * @property {boolean} [verbose=false]
 * @property {string} [defaultName='<stdin>']
 *
 * @typedef Row
 * @property {string} place
 * @property {string} label
 * @property {string} reason
 * @property {string} ruleId
 * @property {string} source
 *
 * @typedef FileRow
 * @property {'file'} type
 * @property {VFile} file
 * @property {Statistics} stats
 *
 * @typedef {Record<string, number>} Sizes
 *
 * @typedef Info
 * @property {Array<FileRow|Row>} rows
 * @property {Statistics} stats
 * @property {Sizes} sizes
 */

import width from 'string-width'
import {stringifyPosition} from 'unist-util-stringify-position'
import {statistics} from 'vfile-statistics'
import {sort} from 'vfile-sort'
import {color} from './color.js'
import {platform} from './platform.js'

const own = {}.hasOwnProperty

// `log-symbols` without chalk, ignored for Windows:
/* c8 ignore next 4 */
const chars =
  platform === 'win32' ? {error: '×', warning: '‼'} : {error: '✖', warning: '⚠'}

const labels = {
  true: 'error',
  false: 'warning',
  null: 'info',
  undefined: 'info'
}

/**
 * Report a file’s messages.
 *
 * @param {Error|VFile|Array<VFile>} [files]
 * @param {Options} [options]
 * @returns {string}
 */
export function reporter(files, options = {}) {
  /** @type {boolean|undefined} */
  let one

  if (!files) {
    return ''
  }

  // Error.
  if ('name' in files && 'message' in files) {
    return String(files.stack || files)
  }

  // One file.
  if (!Array.isArray(files)) {
    one = true
    files = [files]
  }

  return format(transform(files, options), one, options)
}

/**
 * @param {Array<VFile>} files
 * @param {Options} options
 * @returns {Info}
 */
function transform(files, options) {
  /** @type {Array<FileRow|Row>} */
  const rows = []
  /** @type {Array<VFileMessage>} */
  const all = []
  /** @type {Sizes} */
  const sizes = {}
  let index = -1

  while (++index < files.length) {
    // @ts-expect-error it works fine.
    const messages = sort({messages: [...files[index].messages]}).messages
    /** @type {Array<Row>} */
    const messageRows = []
    let offset = -1

    while (++offset < messages.length) {
      const message = messages[offset]

      if (!options.silent || message.fatal) {
        all.push(message)

        const row = {
          place: stringifyPosition(
            message.position
              ? message.position.end.line && message.position.end.column
                ? message.position
                : message.position.start
              : undefined
          ),
          label: labels[/** @type {keyof labels} */ (String(message.fatal))],
          reason:
            (message.stack || message.message) +
            (options.verbose && message.note ? '\n' + message.note : ''),
          ruleId: message.ruleId || '',
          source: message.source || ''
        }

        /** @type {keyof row} */
        let key

        for (key in row) {
          // eslint-disable-next-line max-depth
          if (own.call(row, key)) {
            sizes[key] = Math.max(size(row[key]), sizes[key] || 0)
          }
        }

        messageRows.push(row)
      }
    }

    if ((!options.quiet && !options.silent) || messageRows.length > 0) {
      rows.push(
        {type: 'file', file: files[index], stats: statistics(messages)},
        ...messageRows
      )
    }
  }

  return {rows, stats: statistics(all), sizes}
}

/**
 * @param {Info} map
 * @param {boolean|undefined} one
 * @param {Options} options
 */
// eslint-disable-next-line complexity
function format(map, one, options) {
  /** @type {boolean} */
  const enabled =
    options.color === undefined || options.color === null
      ? color
      : options.color
  /** @type {Array<string>} */
  const lines = []
  let index = -1

  while (++index < map.rows.length) {
    const row = map.rows[index]

    if ('type' in row) {
      const stats = row.stats
      let line = row.file.history[0] || options.defaultName || '<stdin>'

      line =
        one && !options.defaultName && !row.file.history[0]
          ? ''
          : (enabled
              ? '\u001B[4m' /* Underline. */ +
                (stats.fatal
                  ? '\u001B[31m' /* Red. */
                  : stats.total
                  ? '\u001B[33m' /* Yellow. */
                  : '\u001B[32m') /* Green. */ +
                line +
                '\u001B[39m\u001B[24m'
              : line) +
            (row.file.stored && row.file.path !== row.file.history[0]
              ? ' > ' + row.file.path
              : '')

      if (!stats.total) {
        line =
          (line ? line + ': ' : '') +
          (row.file.stored
            ? enabled
              ? '\u001B[33mwritten\u001B[39m' /* Yellow. */
              : 'written'
            : 'no issues found')
      }

      if (line) {
        if (index && !('type' in map.rows[index - 1])) {
          lines.push('')
        }

        lines.push(line)
      }
    } else {
      let reason = row.reason
      const match = /\r?\n|\r/.exec(reason)
      /** @type {string} */
      let rest

      if (match) {
        rest = reason.slice(match.index)
        reason = reason.slice(0, match.index)
      } else {
        rest = ''
      }

      lines.push(
        (
          '  ' +
          ' '.repeat(map.sizes.place - size(row.place)) +
          row.place +
          '  ' +
          (enabled
            ? (row.label === 'error'
                ? '\u001B[31m' /* Red. */
                : '\u001B[33m') /* Yellow. */ +
              row.label +
              '\u001B[39m'
            : row.label) +
          ' '.repeat(map.sizes.label - size(row.label)) +
          '  ' +
          reason +
          ' '.repeat(map.sizes.reason - size(reason)) +
          '  ' +
          row.ruleId +
          ' '.repeat(map.sizes.ruleId - size(row.ruleId)) +
          '  ' +
          (row.source || '')
        ).replace(/ +$/, '') + rest
      )
    }
  }

  const stats = map.stats

  if (stats.fatal || stats.warn) {
    let line = ''

    if (stats.fatal) {
      line =
        (enabled
          ? '\u001B[31m' /* Red. */ + chars.error + '\u001B[39m'
          : chars.error) +
        ' ' +
        stats.fatal +
        ' ' +
        (labels.true + (stats.fatal === 1 ? '' : 's'))
    }

    if (stats.warn) {
      line =
        (line ? line + ', ' : '') +
        (enabled
          ? '\u001B[33m' /* Yellow. */ + chars.warning + '\u001B[39m'
          : chars.warning) +
        ' ' +
        stats.warn +
        ' ' +
        (labels.false + (stats.warn === 1 ? '' : 's'))
    }

    if (stats.total !== stats.fatal && stats.total !== stats.warn) {
      line = stats.total + ' messages (' + line + ')'
    }

    lines.push('', line)
  }

  return lines.join('\n')
}

/**
 * Get the length of `value`, ignoring ANSI sequences.
 *
 * @param {string} value
 * @returns {number}
 */
function size(value) {
  const match = /\r?\n|\r/.exec(value)
  return width(match ? value.slice(0, match.index) : value)
}
