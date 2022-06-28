/**
 *  Load the plugin found using `resolvePlugin`.
 *
 * @param {string} name The name to import.
 * @param {LoadOptions} [options]
 * @returns {Promise<unknown>}
 */
export function loadPlugin(
  name: string,
  options?: LoadOptions | undefined
): Promise<unknown>
/**
 * Find a plugin.
 *
 * See also:
 * *   https://docs.npmjs.com/files/folders#node-modules
 * *   https://github.com/sindresorhus/resolve-from
 *
 * Uses the standard node module loading strategy to find `$name` in each given
 * `cwd` (and optionally the global `node_modules` directory).
 *
 * If a prefix is given and `$name` is not a path, `$prefix-$name` is also
 * searched (preferring these over non-prefixed modules).
 *
 * @param {string} name
 * @param {ResolveOptions} [options]
 * @returns {Promise<string>}
 */
export function resolvePlugin(
  name: string,
  options?: ResolveOptions | undefined
): Promise<string>
export type ResolveOptions = {
  prefix?: string | undefined
  cwd?: string | string[] | undefined
  global?: boolean | undefined
}
export type LoadOptions = ResolveOptions & {
  key?: string | false
}
