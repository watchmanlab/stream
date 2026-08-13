export const EMPTY = Symbol.for("empty");
export const EMPTY_FUNCTION = () => {};
export const EMPTY_THIS_FUNCTION = function (this: any) {
  return this;
};
