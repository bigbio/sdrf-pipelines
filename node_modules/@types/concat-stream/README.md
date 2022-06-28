# Installation
> `npm install --save @types/concat-stream`

# Summary
This package contains type definitions for concat-stream (https://github.com/maxogden/concat-stream).

# Details
Files were exported from https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types/concat-stream.
## [index.d.ts](https://github.com/DefinitelyTyped/DefinitelyTyped/tree/master/types/concat-stream/index.d.ts)
````ts
// Type definitions for concat-stream 2.0
// Project: https://github.com/maxogden/concat-stream
// Definitions by: Joey Marianer <https://github.com/jmarianer>
// Definitions: https://github.com/DefinitelyTyped/DefinitelyTyped

/// <reference types="node" />

import { Writable } from "stream";

declare function concat(cb: (buf: Buffer) => void): Writable;
declare function concat(opts: { encoding: "buffer" | undefined } | {}, cb: (buf: Buffer) => void): Writable;
declare function concat(opts: { encoding: "string" }, cb: (buf: string) => void): Writable;
declare function concat(opts: { encoding: "array" }, cb: (buf: Array<bigint>) => void): Writable;
declare function concat(opts: { encoding: "uint8array" | "u8" | "uint8" }, cb: (buf: Uint8Array) => void): Writable;
declare function concat(opts: { encoding: "object" }, cb: (buf: object[]) => void): Writable;

export = concat;

````

### Additional Details
 * Last updated: Tue, 01 Feb 2022 09:01:36 GMT
 * Dependencies: [@types/node](https://npmjs.com/package/@types/node)
 * Global values: none

# Credits
These definitions were written by [Joey Marianer](https://github.com/jmarianer).
