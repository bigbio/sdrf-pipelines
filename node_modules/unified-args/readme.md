# unified-args

[![Build][build-badge]][build]
[![Coverage][coverage-badge]][coverage]
[![Downloads][downloads-badge]][downloads]
[![Sponsors][sponsors-badge]][collective]
[![Backers][backers-badge]][collective]
[![Chat][chat-badge]][chat]

**[unified][]** engine to create a command line interface from a unified
processor.

## Contents

*   [What is this?](#what-is-this)
*   [When should I use this?](#when-should-i-use-this)
*   [Install](#install)
*   [Use](#use)
*   [API](#api)
    *   [`args(configuration)`](#argsconfiguration)
*   [CLI](#cli)
    *   [Files](#files)
    *   [`--help`](#--help)
    *   [`--version`](#--version)
    *   [`--output [path]`](#--output-path)
    *   [`--rc-path <path>`](#--rc-path-path)
    *   [`--ignore-path <path>`](#--ignore-path-path)
    *   [`--ignore-path-resolve-from dir|cwd`](#--ignore-path-resolve-from-dircwd)
    *   [`--ignore-pattern <globs>`](#--ignore-pattern-globs)
    *   [`--silently-ignore`](#--silently-ignore)
    *   [`--setting <settings>`](#--setting-settings)
    *   [`--report <reporter>`](#--report-reporter)
    *   [`--use <plugin>`](#--use-plugin)
    *   [`--ext <extensions>`](#--ext-extensions)
    *   [`--watch`](#--watch)
    *   [`--tree`](#--tree)
    *   [`--tree-in`](#--tree-in)
    *   [`--tree-out`](#--tree-out)
    *   [`--inspect`](#--inspect)
    *   [`--quiet`](#--quiet)
    *   [`--silent`](#--silent)
    *   [`--frail`](#--frail)
    *   [`--file-path <path>`](#--file-path-path)
    *   [`--stdout`](#--stdout)
    *   [`--color`](#--color)
    *   [`--config`](#--config)
    *   [`--ignore`](#--ignore)
*   [Diagnostics](#diagnostics)
*   [Debugging](#debugging)
*   [Types](#types)
*   [Compatibility](#compatibility)
*   [Security](#security)
*   [Contribute](#contribute)
*   [License](#license)

## What is this?

This package wraps [`unified-engine`][unified-engine] so that it can be used
to create a command line interface.
It’s what you use underneath when you use [`remark-cli`][remark-cli].

## When should I use this?

You can use this to let users process multiple files from the command line,
letting them configure from the file system.

## Install

This package is [ESM only][esm].
In Node.js (version 14.14+, 16.0+, or 18.0+), install with [npm][]:

```sh
npm install unified-args
```

## Use

The following example creates a CLI for [remark][], which will search for files
in folders with a markdown extension, allows [configuration][config-file] from
`.remarkrc` and `package.json` files, [ignoring files][ignore-file] from
`.remarkignore` files, and more.

Say our module `example.js` looks as follows:

```js
import {args} from 'unified-args'
import {remark} from 'remark'

args({
  processor: remark,
  name: 'remark',
  description:
    'Command line interface to inspect and change markdown files with remark',
  version: '14.0.0',
  pluginPrefix: 'remark',
  packageField: 'remarkConfig',
  rcName: '.remarkrc',
  ignoreName: '.remarkignore',
  extensions: [
    'md',
    'markdown',
    'mdown',
    'mkdn',
    'mkd',
    'mdwn',
    'mkdown',
    'ron'
  ]
})
```

…now running `node example.js --help` yields:

```txt
Usage: remark [options] [path | glob ...]

  Command line interface to inspect and change markdown files with remark

Options:

  -h  --help                              output usage information
  …
```

## API

This package exports the identifier `args`.
There is no default export.

### `args(configuration)`

Create a command line interface from a unified processor.

###### `configuration`

All options are required.

*   `processor` ([`Processor`][unified-processor])
    — processor to inspect and transform files
    (engine: [`processor`][engine-processor])
*   `name` (`string`)
    — name of executable
*   `description` (`string`)
    — description of executable
*   `version` (`string`)
    — version of executable
*   `extensions` (`Array<string>`)
    — default file [extensions][ext] to include
    (engine: [`extensions`][engine-extensions])
*   `ignoreName` (`string`)
    — name of [ignore files][ignore-file] to load
    (engine: [`ignoreName`][engine-ignore-name])
*   `rcName` (`string`)
    — name of [configuration files][config-file] to load
    (engine: [`rcName`][engine-rc-name])
*   `packageField` (`string`)
    — field where [configuration][config-file] can be found in `package.json`s
    (engine: [`packageField`][engine-package-field])
*   `pluginPrefix` (`string`)
    — prefix to use when searching for [plugins][use]
    (engine: [`pluginPrefix`][engine-plugin-prefix])

## CLI

CLIs created with `unified-args`, such as the [example][] above, have an
interface similar to the below:

```txt
Usage: remark [options] [path | glob ...]

  Command line interface to inspect and change markdown files with remark

Options:

  -h  --help                              output usage information
  -v  --version                           output version number
  -o  --output [path]                     specify output location
  -r  --rc-path <path>                    specify configuration file
  -i  --ignore-path <path>                specify ignore file
  -s  --setting <settings>                specify settings
  -e  --ext <extensions>                  specify extensions
  -u  --use <plugins>                     use plugins
  -w  --watch                             watch for changes and reprocess
  -q  --quiet                             output only warnings and errors
  -S  --silent                            output only errors
  -f  --frail                             exit with 1 on warnings
  -t  --tree                              specify input and output as syntax tree
      --report <reporter>                 specify reporter
      --file-path <path>                  specify path to process as
      --ignore-path-resolve-from dir|cwd  resolve patterns in `ignore-path` from its directory or cwd
      --ignore-pattern <globs>            specify ignore patterns
      --silently-ignore                   do not fail when given ignored files
      --tree-in                           specify input as syntax tree
      --tree-out                          output syntax tree
      --inspect                           output formatted syntax tree
      --[no-]stdout                       specify writing to stdout (on by default)
      --[no-]color                        specify color in report (on by default)
      --[no-]config                       search for configuration files (on by default)
      --[no-]ignore                       search for ignore files (on by default)

Examples:

  # Process `input.md`
  $ remark input.md -o output.md

  # Pipe
  $ remark < input.md > output.md

  # Rewrite all applicable files
  $ remark . -o
```

### Files

All non-options passed to the cli are seen as input and can be:

*   paths (`readme.txt`) and [globs][glob] (`*.txt`) pointing to files to load
*   paths (`test`) and globs (`fixtures/{in,out}`) pointing to folders, which
    are searched for files with known [extensions][ext] which are not ignored
    by patterns in [ignore files][ignore-file].
    The default behavior is to exclude files in `node_modules` and hidden
    folders (those starting with `.`) unless explicitly given

You can force things to be seen as input by using `--`:

```sh
cli -- globs/* and/files
```

*   **default**: none
*   **engine**: [`files`][engine-files]

### `--help`

```sh
cli --help
```

Output short usage information.

*   **default**: off
*   **alias**: `-h`

### `--version`

```sh
cli --version
```

Output version number.

*   **default**: off
*   **alias**: `-v`

### `--output [path]`

```sh
cli --output -- .
cli --output doc .
cli --output doc/output.text input.txt
```

Whether to write successfully processed files, and where to.
Can be set from [configuration files][config-file].

*   if output is not given, files are not written to the file system
*   otherwise, if `path` is not given, files are overwritten when successful
*   otherwise, if `path` points to a folder, files are written there
*   otherwise, if one file is processed, the file is written to `path`

> 👉 **Note**: intermediate folders are not created.

*   **default**: off
*   **alias**: `-o`
*   **engine**: [`output`][engine-output]

### `--rc-path <path>`

```sh
cli --rc-path config.json .
```

File path to a [configuration file][config-file] to load, regardless of
[`--config`][config].

*   **default**: none
*   **alias**: `-r`
*   **engine**: [`rcPath`][engine-rc-path]

### `--ignore-path <path>`

```sh
cli --ignore-path .gitignore .
```

File path to an [ignore file][ignore-file] to load, regardless of
[`--ignore`][ignore].

*   **default**: none
*   **alias**: `-i`
*   **engine**: [`ignorePath`][engine-ignore-path]

### `--ignore-path-resolve-from dir|cwd`

```sh
cli --ignore-path node_modules/my-config/my-ignore --ignore-path-resolve-from cwd .
```

Resolve patterns in the ignore file from its directory (`dir`, default) or the
current working directory (`cwd`).

*   **default**: `dir`
*   **engine**: [`ignorePathResolveFrom`][engine-ignore-path-resolve-from]

### `--ignore-pattern <globs>`

```sh
cli --ignore-pattern docs/*.md .
```

Additional patterns to use to ignore files.

*   **default**: none
*   **engine**: [`ignorePatterns`][engine-ignore-patterns]

### `--silently-ignore`

```sh
cli --silently-ignore **/*.md
```

Skip given files which are ignored by ignore files, instead of warning about
them.

*   **default**: off
*   **engine**: [`silentlyIgnore`][engine-silently-ignore]

### `--setting <settings>`

```sh
cli --setting alpha:true input.txt
cli --setting bravo:true --setting '"charlie": "delta"' input.txt
cli --setting echo-foxtrot:-2 input.txt
cli --setting 'golf: false, hotel-india: ["juliet", 1]' input.txt
```

Configuration for the parser and compiler of the processor.
Can be set from [configuration files][config-file].

The given settings are [JSON5][], with one exception: surrounding braces must
not be used.  Instead, use JSON syntax without braces, such as
`"foo": 1, "bar": "baz"`.

*   **default**: none
*   **alias**: `-s`
*   **engine**: [`settings`][engine-settings]

### `--report <reporter>`

```sh
cli --report ./reporter.js input.txt
cli --report vfile-reporter-json input.txt
cli --report json input.txt
cli --report json=pretty:2 input.txt
cli --report 'json=pretty:"\t"' input.txt
# Only last one is used:
cli --report pretty --report json input.txt
```

[Reporter][] to load by its name or path, optionally with options, and use to
report metadata about every processed file.

To pass options, follow the name by an equals sign (`=`) and settings, which
have the same in syntax as [`--setting <settings>`][setting].

The prefix `vfile-reporter-` can be omitted.
Prefixed reporters are preferred over modules without prefix.

If multiple reporters are given, the last one is used.

*   **default**: none, which uses [`vfile-reporter`][vfile-reporter]
*   **engine**: [`reporter`][engine-reporter] and
    [`reporterOptions`][engine-reporter-options]

> 👉 **Note**: the [`quiet`][quiet], [`silent`][silent], and [`color`][color]
> options may not work with the used reporter.
> If they are given, they are preferred over the same properties in reporter
> settings.

### `--use <plugin>`

```sh
cli --use remark-man input.txt
cli --use man input.txt
cli --use 'toc=max-depth:3' input.txt
cli --use ./plugin.js input.txt
```

Plugin to load by its name or path, optionally with options, and use on every
processed file.
Can be set from [configuration files][config-file].

To pass options, follow the plugin by an equals sign (`=`) and settings, which
have the same in syntax as [`--setting <settings>`][setting].

Plugins prefixed with the [configured `pluginPrefix`][configured] are preferred
over modules without prefix.

*   **default**: none
*   **alias**: `-u`
*   **engine**: [`plugins`][engine-plugins]

### `--ext <extensions>`

```sh
cli --ext html .
cli --ext html --ext htm .
cli --ext html,htm .
```

Specify one or more extensions to include when searching for files.

If no extensions are given, uses the [configured `extensions`][configured].

*   **default**: configured [`extensions`][configured]
*   **alias**: `-e`
*   **engine**: [`extensions`][engine-extensions]

### `--watch`

```sh
cli -qwo .
```

Yields:

```txt
Watching... (press CTRL+C to exit)
Note: Ignoring `--output` until exit.
```

Process as normal, then watch found files and reprocess when they change.
The watch is stopped when `SIGINT` is received (usually done by pressing
`CTRL-C`).

If [`--output`][output] is given without `path`, it is not honored, to prevent
an infinite loop.
On operating systems other than Windows, when the watch closes, a final process
runs including `--output`.

*   **default**: off
*   **alias**: `-w`

### `--tree`

```sh
cli --tree < input.json > output.json
```

Treat input as a syntax tree in JSON and output the transformed syntax tree.
This runs neither the [parsing nor the compilation phase][description].

*   **default**: off
*   **alias**: `-t`
*   **engine**: [`tree`][engine-tree]

### `--tree-in`

```sh
cli --tree-in < input.json > input.txt
```

Treat input as a syntax tree in JSON.
This does not run the [parsing phase][description].

*   **default**: off
*   **engine**: [`treeIn`][engine-tree-in]

### `--tree-out`

```sh
cli --tree-out < input.txt > output.json
```

Output the transformed syntax tree.
This does not run the [compilation phase][description].

*   **default**: off
*   **engine**: [`treeOut`][engine-tree-out]

### `--inspect`

```sh
cli --inspect < input.txt
```

Output the transformed syntax tree, formatted with
[`unist-util-inspect`][unist-util-inspect].
This does not run the [compilation phase][description].

*   **default**: off
*   **engine**: [`inspect`][engine-inspect]

### `--quiet`

```sh
cli --quiet input.txt
```

Ignore files without any messages in the report.
The default behavior is to show a success message.

*   **default**: off
*   **alias**: `-q`
*   **engine**: [`quiet`][engine-quiet]

> 👉 **Note**: this option may not work depending on the reporter given in
> [`--report`][report].

### `--silent`

```sh
cli --silent input.txt
```

Show only fatal errors in the report.
Turns [`--quiet`][quiet] on.

*   **default**: off
*   **alias**: `-S`
*   **engine**: [`silent`][engine-silent]

> 👉 **Note**: this option may not work depending on the reporter given in
> [`--report`][report].

### `--frail`

```sh
cli --frail input.txt
```

Exit with a status code of `1` if warnings or errors occur.
The default behavior is to exit with `1` on errors.

*   **default**: off
*   **alias**: `-f`
*   **engine**: [`frail`][engine-frail]

### `--file-path <path>`

```sh
cli --file-path input.txt < input.txt > doc/output.txt
```

File path to process the given file on **stdin**(4) as, if any.

*   **default**: none
*   **engine**: [`filePath`][engine-file-path]

### `--stdout`

```sh
cli --no-stdout input.txt
```

Whether to write a processed file to **stdout**(4).

*   **default**: off if [`--output`][output] or [`--watch`][watch] are given, or
    if multiple files could be processed
*   **engine**: [`out`][engine-out]

### `--color`

```sh
cli --no-color input.txt
```

Whether to output ANSI color codes in the report.

*   **default**: whether the terminal [supports color][supports-color]
*   **engine**: [`color`][engine-color]

> 👉 **Note**: This option may not work depending on the reporter given in
> [`--report`][report].

### `--config`

```sh
cli --no-config input.txt
```

Whether to load [configuration files][config-file].

Searches for files with the [configured `rcName`][configured]: `$rcName` and
`$rcName.json` (JSON), `$rcName.yml` and  `$rcName.yaml` (YAML), `$rcName.js`
(JavaScript), `$rcName.cjs` (CommonJS), and `$rcName.mjs` (ESM); and looks for
the [configured `packageField`][configured] in `package.json` files.

*   **default**: on
*   **engine**: [`detectConfig`][engine-detect-config]

### `--ignore`

```sh
cli --no-ignore .
```

Whether to load [ignore files][ignore-file].

Searches for files named [`$ignoreName`][configured].

*   **default**: on
*   **engine**: [`detectIgnore`][engine-detect-ignore]

## Diagnostics

CLIs created with **unified-args** exit with:

*   `1` on fatal errors
*   `1` on warnings in [`--frail`][frail] mode, `0` on warnings otherwise
*   `0` on success

## Debugging

CLIs can be debugged by setting the [`DEBUG`][debug] environment variable to
`*`, such as `DEBUG="*" cli example.txt`.

## Types

This package is fully typed with [TypeScript][].
It export the additional type `Options`.

## Compatibility

Projects maintained by the unified collective are compatible with all maintained
versions of Node.js.
As of now, that is Node.js 14.14+, 16.0+, and 18.0+.
Our projects sometimes work with older versions, but this is not guaranteed.

## Security

`unified-args` loads and evaluates configuration files, plugins, and presets
from the file system (often from `node_modules/`).
That means code that is on your file system runs.
Make sure you trust the workspace where you run `unified-args` and be careful
with packages from npm and changes made by contributors.

## Contribute

See [`contributing.md`][contributing] in [`unifiedjs/.github`][health] for ways
to get started.
See [`support.md`][support] for ways to get help.

This project has a [code of conduct][coc].
By interacting with this repository, organization, or community you agree to
abide by its terms.

## License

[MIT][license] © [Titus Wormer][author]

<!-- Definitions -->

[build-badge]: https://github.com/unifiedjs/unified-args/workflows/main/badge.svg

[build]: https://github.com/unifiedjs/unified-args/actions

[coverage-badge]: https://img.shields.io/codecov/c/github/unifiedjs/unified-args.svg

[coverage]: https://codecov.io/github/unifiedjs/unified-args

[downloads-badge]: https://img.shields.io/npm/dm/unified-args.svg

[downloads]: https://www.npmjs.com/package/unified-args

[sponsors-badge]: https://opencollective.com/unified/sponsors/badge.svg

[backers-badge]: https://opencollective.com/unified/backers/badge.svg

[collective]: https://opencollective.com/unified

[chat-badge]: https://img.shields.io/badge/chat-discussions-success.svg

[chat]: https://github.com/unifiedjs/unified/discussions

[npm]: https://docs.npmjs.com/cli/install

[esm]: https://gist.github.com/sindresorhus/a39789f98801d908bbc7ff3ecc99d99c

[typescript]: https://www.typescriptlang.org

[health]: https://github.com/unifiedjs/.github

[contributing]: https://github.com/unifiedjs/.github/blob/main/contributing.md

[support]: https://github.com/unifiedjs/.github/blob/main/support.md

[coc]: https://github.com/unifiedjs/.github/blob/main/code-of-conduct.md

[license]: license

[author]: https://wooorm.com

[unified]: https://github.com/unifiedjs/unified

[unified-processor]: https://github.com/unifiedjs/unified#processor

[description]: https://github.com/unifiedjs/unified#description

[remark]: https://github.com/remarkjs/remark

[remark-cli]: https://github.com/remarkjs/remark/tree/main/packages/remark-cli

[reporter]: https://github.com/vfile/vfile#reporters

[vfile-reporter]: https://github.com/vfile/vfile-reporter

[unist-util-inspect]: https://github.com/syntax-tree/unist-util-inspect

[debug]: https://github.com/debug-js/debug

[glob]: https://github.com/isaacs/node-glob#glob-primer

[supports-color]: https://github.com/chalk/supports-color

[json5]: https://github.com/json5/json5

[unified-engine]: https://github.com/unifiedjs/unified-engine

[config-file]: https://github.com/unifiedjs/unified-engine/blob/main/doc/configure.md

[ignore-file]: https://github.com/unifiedjs/unified-engine/blob/main/doc/ignore.md

[engine-processor]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsprocessor

[engine-files]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsfiles

[engine-output]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsoutput

[engine-rc-path]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsrcpath

[engine-rc-name]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsrcname

[engine-package-field]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionspackagefield

[engine-ignore-name]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsignorename

[engine-ignore-path]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsignorepath

[engine-ignore-path-resolve-from]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsignorepathresolvefrom

[engine-ignore-patterns]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsignorepatterns

[engine-silently-ignore]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionssilentlyignore

[engine-reporter]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsreporter

[engine-reporter-options]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsreporteroptions

[engine-settings]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionssettings

[engine-plugins]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsplugins

[engine-plugin-prefix]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionspluginprefix

[engine-extensions]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsextensions

[engine-tree]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionstree

[engine-tree-in]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionstreein

[engine-tree-out]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionstreeout

[engine-inspect]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsinspect

[engine-quiet]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsquiet

[engine-silent]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsilent

[engine-frail]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsfrail

[engine-file-path]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsfilepath

[engine-out]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsout

[engine-color]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionscolor

[engine-detect-config]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsdetectconfig

[engine-detect-ignore]: https://github.com/unifiedjs/unified-engine/blob/main/doc/options.md#optionsdetectignore

[configured]: #argsconfiguration

[example]: #use

[watch]: #--watch

[output]: #--output-path

[config]: #--config

[ext]: #--ext-extensions

[use]: #--use-plugin

[ignore]: #--ignore

[setting]: #--setting-settings

[report]: #--report-reporter

[quiet]: #--quiet

[silent]: #--silent

[color]: #--color

[frail]: #--frail
