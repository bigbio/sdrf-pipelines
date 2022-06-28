/// <reference types="node" />
/**
 * @param {URL} url
 * @returns {PackageType}
 */
export function getPackageType(url: URL): PackageType
/**
 * The “Resolver Algorithm Specification” as detailed in the Node docs (which is
 * sync and slightly lower-level than `resolve`).
 *
 * @param {string} specifier
 * @param {URL} base
 * @param {Set<string>} [conditions]
 * @param {boolean} [preserveSymlinks]
 * @returns {URL}
 */
export function moduleResolve(
  specifier: string,
  base: URL,
  conditions?: Set<string> | undefined,
  preserveSymlinks?: boolean | undefined
): URL
/**
 * @param {string} specifier
 * @param {{parentURL?: string, conditions?: Array<string>}} context
 * @returns {{url: string, format?: string|null}}
 */
export function defaultResolve(
  specifier: string,
  context?: {
    parentURL?: string | undefined
    conditions?: string[] | undefined
  }
): {
  url: string
  format?: string | null | undefined
}
export type ErrnoException = import('./errors.js').ErrnoException
export type PackageType = 'module' | 'commonjs' | 'none'
export type Format = 'module' | 'commonjs'
export type PackageConfig = {
  pjsonPath: string
  exists: boolean
  main: string | undefined
  name: string | undefined
  type: PackageType
  exports: Record<string, unknown> | undefined
  imports: Record<string, unknown> | undefined
}
import {URL} from 'url'
