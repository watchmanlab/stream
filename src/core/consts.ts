/** Sentinel symbol used to represent the absence of a value. */
export const EMPTY = Symbol.for("empty");

/** A no-op function used to replace handlers after termination to avoid null checks. */
export const EMPTY_FUNCTION = () => {};

/** A no-op method that returns `this`, used to replace chainable methods after termination. */
export const EMPTY_THIS_FUNCTION = function (this: any) {
  return this;
};
