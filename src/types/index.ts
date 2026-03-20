export type FixedArray<
  CLEAN_VALUE,
  SIZE extends number = 2,
  ARR extends Array<CLEAN_VALUE> = [],
> = ARR["length"] extends SIZE ? ARR : FixedArray<CLEAN_VALUE, SIZE, [...ARR, CLEAN_VALUE]>;
