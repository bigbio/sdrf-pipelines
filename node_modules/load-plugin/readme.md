# load-plugin

[![Build][build-badge]][build]
[![Coverage][coverage-badge]][coverage]
[![Downloads][downloads-badge]][downloads]

Load submodules, plugins, or files.

## Contents

*   [What is this?](#what-is-this)
*   [When to use this?](#when-to-use-this)
*   [Install](#install)
*   [Use](#use)
*   [API](#api)
    *   [`loadPlugin(name[, options])`](#loadpluginname-options)
    *   [`resolvePlugin(name[, options])`](#resolvepluginname-options)
*   [Types](#types)
*   [Compatibility](#compatibility)
*   [Contribute](#contribute)
*   [License](#license)

## What is this?

This package is useful when you want to load plugins.
It resolves things like Node.js does, but supports a prefix (e.g., when given a
prefix `remark` and the user provided value `gfm`, it can find `remark-gfm`),
can load from several places, and optionally global too.

## When to use this?

This package is particularly useful when you want users to configure something
with plugins.
One example is `remark-cli` which can load remark plugins from configuration
files.

## Install

This package is [ESM only][esm].
In Node.js (version 14.14+, 16.0+, or 18.0+), install with [npm][]:

```sh
npm install load-plugin
```

## Use

Say we’re in this project (with dependencies installed):

```js
import {loadPlugin, resolvePlugin} from 'load-plugin'

console.log(await resolvePlugin('lint', {prefix: 'remark'}))
// => '/Users/tilde/projects/oss/load-plugin/node_modules/remark-lint/index.js'

console.log(await resolvePlugin('validator-identifier', {prefix: '@babel/helper'}))
// => '/Users/tilde/Projects/oss/load-plugin/node_modules/@babel/helper-validator-identifier/lib/index.js'

console.log(await resolvePlugin('./index.js', {prefix: 'remark'}))
// => '/Users/tilde/projects/oss/load-plugin/index.js'

console.log(await loadPlugin('lint', {prefix: 'remark'}))
// => [Function: remarkLint]
```

## API

This package exports the identifiers `loadPlugin` and `resolvePlugin`.
There is no default export.

### `loadPlugin(name[, options])`

Uses Node’s [resolution algorithm][algo] (through
[`import-meta-resolve`][import-meta-resolve]) to load CJS and ESM packages and
files to import `name` in each given `cwd` (and optionally the global
`node_modules` directory).

If a `prefix` is given and `name` is not a path, `$prefix-$name` is also
searched (preferring these over non-prefixed modules).
If `name` starts with a scope (`@scope/name`), the prefix is applied after it:
`@scope/$prefix-name`.

##### `options`

Configuration (optional).

###### `options.prefix`

Prefix to search for (`string`, optional).

###### `options.cwd`

Place or places to search from (`string`, `Array<string>`, default:
`process.cwd()`).

###### `options.global`

Whether to look for `name` in [global places][global] (`boolean`, optional,
defaults to whether global is detected).
If this is nullish, `load-plugin` will detect if it’s currently running in
global mode: either because it’s in Electron, or because a globally installed
package is running it.

Note: Electron runs its own version of Node instead of your system Node.
That means global packages cannot be found, unless you’ve [set-up][] a [`prefix`
in your `.npmrc`][prefix] or are using [nvm][] to manage your system node.

###### `options.key`

Identifier to take from the exports (`string` or `false`, default: `'default'`).
For example when given `'whatever'`, the value of `export const whatever = 1`
will be returned, when given `'default'`, the value of `export default …` is
used, and when `false` the whole module object is returned.

###### Returns

Promise yielding the results of importing the first path that exists
(`Promise<unknown>`).
The promise rejects if importing an existing path fails, or if no existing
path exists.

### `resolvePlugin(name[, options])`

Search for `name`.
Accepts the same parameters as [`loadPlugin`][load-plugin] (except `key`) but
returns a promise resolving to an absolute URL (`string`) for `name` instead of
importing it.
Throws if `name` cannot be found.

## Types

This package is fully typed with [TypeScript][].
It exports the additional types `ResolveOptions` and `LoadOptions`.

## Compatibility

This package is at least compatible with all maintained versions of Node.js.
As of now, that is Node.js 14.14+, 16.0+, and 18.0+.

## Contribute

Yes please!
See [How to Contribute to Open Source][contribute].

## License

[MIT][license] © [Titus Wormer][author]

<!-- Definitions -->

[build-badge]: https://github.com/wooorm/load-plugin/actions/workflows/main.yml/badge.svg

[build]: https://github.com/wooorm/load-plugin/actions

[coverage-badge]: https://img.shields.io/codecov/c/github/wooorm/load-plugin.svg

[coverage]: https://codecov.io/github/wooorm/load-plugin

[downloads-badge]: https://img.shields.io/npm/dm/load-plugin.svg

[downloads]: https://www.npmjs.com/package/load-plugin

[npm]: https://docs.npmjs.com/cli/install

[license]: license

[author]: https://wooorm.com

[esm]: https://gist.github.com/sindresorhus/a39789f98801d908bbc7ff3ecc99d99c

[typescript]: https://www.typescriptlang.org

[contribute]: https://opensource.guide/how-to-contribute/

[global]: https://docs.npmjs.com/files/folders#node-modules

[prefix]: https://docs.npmjs.com/misc/config#prefix

[set-up]: https://github.com/sindresorhus/guides/blob/master/npm-global-without-sudo.md

[nvm]: https://github.com/creationix/nvm

[algo]: https://nodejs.org/api/esm.html#esm_resolution_algorithm

[import-meta-resolve]: https://github.com/wooorm/import-meta-resolve

[load-plugin]: #loadpluginname-options
