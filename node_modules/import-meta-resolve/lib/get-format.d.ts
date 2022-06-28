/// <reference types="node" />
/**
 * @param {URL} url
 * @param {{parentURL: string}} context
 * @returns {string|null}
 */
export function defaultGetFormatWithoutErrors(
  url: URL,
  context: {
    parentURL: string
  }
): string | null
/**
 * @param {string} url
 * @param {{parentURL: string}} context
 * @returns {string|null}
 */
export function defaultGetFormat(
  url: string,
  context: {
    parentURL: string
  }
): string | null
export type ProtocolHandler = (
  parsed: URL,
  context: {
    parentURL: string
  },
  ignoreErrors: boolean
) => string | null
import {URL} from 'url'
