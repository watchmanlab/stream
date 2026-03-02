import { statefull } from "../state-map/state-map";

/**
 * Add index to values
 *
 * @example
 * ```typescript
 * stream.pipe(withIndex())
 * ```
 */
export const withIndex = <T>() =>
  statefull<T, { index: number }, { value: T; index: number }>({ index: 0 }, (state, value) => [
    { value, index: state.index },
    { index: state.index + 1 },
  ]);
