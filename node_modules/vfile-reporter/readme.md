# vfile-reporter

[![Build][build-badge]][build]
[![Coverage][coverage-badge]][coverage]
[![Downloads][downloads-badge]][downloads]
[![Sponsors][sponsors-badge]][collective]
[![Backers][backers-badge]][collective]
[![Chat][chat-badge]][chat]

[vfile][] utility to create a report from a file.

![Example screenshot of vfile-reporter][screenshot]

## Contents

*   [What is this?](#what-is-this)
*   [When should I use this?](#when-should-i-use-this)
*   [Install](#install)
*   [Use](#use)
*   [API](#api)
    *   [`reporter(files[, options])`](#reporterfiles-options)
*   [Types](#types)
*   [Compatibility](#compatibility)
*   [Security](#security)
*   [Related](#related)
*   [Contribute](#contribute)
*   [License](#license)

## What is this?

This package create a textual report from a file showing the warnings that
occurred while processing.
Many CLIs of tools that process files, whether linters (such as ESLint) or
bundlers (such as esbuild), have similar functionality.

## When should I use this?

You can use this package whenever you want to display a report about what
occurred while processing to a human.

## Install

This package is [ESM only][esm].
In Node.js (version 12.20+, 14.14+, or 16.0+), install with [npm][]:

```sh
npm install vfile-reporter
```

In Deno with [`esm.sh`][esmsh]:

```js
import {reporter} from 'https://esm.sh/vfile-reporter@7'
```

In browsers with [`esm.sh`][esmsh]:

```html
<script type="module">
  import {reporter} from 'https://esm.sh/vfile-reporter@7?bundle'
</script>
```

## Use

Say `example.js` contains:

```js
import {VFile} from 'vfile'
import {reporter} from 'vfile-reporter'

const one = new VFile({path: 'test/fixture/1.js'})
const two = new VFile({path: 'test/fixture/2.js'})

one.message('Warning!', {line: 2, column: 4})

console.error(reporter([one, two]))
```

Now, running `node example` yields:

```txt
test/fixture/1.js
  2:4  warning  Warning!

test/fixture/2.js: no issues found

⚠ 1 warning
```

## API

This package exports the identifier `reporter`.
That value is also the default export.

### `reporter(files[, options])`

Create a report from an error, on file, or multiple files.

##### `options`

Configuration (optional).

###### `options.color`

Use ANSI colors in report (`boolean`, default: depends).
The default behavior is the check if [color is supported][supports-color].

###### `options.verbose`

Show message [`note`][message-note]s (`boolean`, default: `false`).
Notes are optional, additional, long descriptions.

###### `options.quiet`

Do not show files without messages (`boolean`, default: `false`).

###### `options.silent`

Show errors only (`boolean`, default: `false`).
This does not show info and warning messages.
Also sets `quiet` to `true`.

###### `options.defaultName`

Label to use for files without file path (`string`, default: `'<stdin>'`).
If one file and no `defaultName` is given, no name will show up in the report.

##### Returns

`string`.

## Types

This package is fully typed with [TypeScript][].
It exports the additional type `Options`.

## Compatibility

Projects maintained by the unified collective are compatible with all maintained
versions of Node.js.
As of now, that is Node.js 12.20+, 14.14+, and 16.0+.
Our projects sometimes work with older versions, but this is not guaranteed.

## Security

Use of `vfile-reporter` is safe.

## Related

*   [`vfile-reporter-json`](https://github.com/vfile/vfile-reporter-json)
    — create a JSON report
*   [`vfile-reporter-pretty`](https://github.com/vfile/vfile-reporter-pretty)
    — create a pretty report
*   [`vfile-reporter-junit`](https://github.com/kellyselden/vfile-reporter-junit)
    — create a jUnit report
*   [`vfile-reporter-position`](https://github.com/Hocdoc/vfile-reporter-position)
    — create a report with content excerpts

## Contribute

See [`contributing.md`][contributing] in [`vfile/.github`][health] for ways to
get started.
See [`support.md`][support] for ways to get help.

This project has a [code of conduct][coc].
By interacting with this repository, organisation, or community you agree to
abide by its terms.

## License

[MIT][license] © [Titus Wormer][author]

Forked from [ESLint][]’s stylish reporter
(originally created by Sindre Sorhus), which is Copyright (c) 2013
Nicholas C. Zakas, and licensed under MIT.

<!-- Definitions -->

[build-badge]: https://github.com/vfile/vfile-reporter/workflows/main/badge.svg

[build]: https://github.com/vfile/vfile-reporter/actions

[coverage-badge]: https://img.shields.io/codecov/c/github/vfile/vfile-reporter.svg

[coverage]: https://codecov.io/github/vfile/vfile-reporter

[downloads-badge]: https://img.shields.io/npm/dm/vfile-reporter.svg

[downloads]: https://www.npmjs.com/package/vfile-reporter

[sponsors-badge]: https://opencollective.com/unified/sponsors/badge.svg

[backers-badge]: https://opencollective.com/unified/backers/badge.svg

[collective]: https://opencollective.com/unified

[chat-badge]: https://img.shields.io/badge/chat-discussions-success.svg

[chat]: https://github.com/vfile/vfile/discussions

[npm]: https://docs.npmjs.com/cli/install

[esm]: https://gist.github.com/sindresorhus/a39789f98801d908bbc7ff3ecc99d99c

[esmsh]: https://esm.sh

[typescript]: https://www.typescriptlang.org

[contributing]: https://github.com/vfile/.github/blob/main/contributing.md

[support]: https://github.com/vfile/.github/blob/main/support.md

[health]: https://github.com/vfile/.github

[coc]: https://github.com/vfile/.github/blob/main/code-of-conduct.md

[license]: license

[author]: https://wooorm.com

[eslint]: https://github.com/eslint/eslint

[vfile]: https://github.com/vfile/vfile

[screenshot]: ./screenshot.png

[supports-color]: https://github.com/chalk/supports-color

[message-note]: https://github.com/vfile/vfile-message#note
