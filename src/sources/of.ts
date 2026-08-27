import { fromIterable, type IterableSource } from "./iterable-source";

export function of<VALUE>(...values: [value: VALUE, ...values: VALUE[]]): IterableSource<VALUE> {
  return fromIterable(values);
}
