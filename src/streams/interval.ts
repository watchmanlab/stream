import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class Interval<MS extends number, NAME extends NonEmptyString = `$interval${MS}ms`> extends Stream<void, NAME> {
  constructor(ms: MS, options?: Stream.Options<void, NAME>) {
    const { name, terminate, ...rest } = options ?? {};

    const timer = setInterval(() => this.push(), ms);
    super({
      ...rest,
      name: name ?? (`$interval${ms}ms` as NAME),
      terminate(self, reason) {
        clearInterval(timer);
        terminate?.(self, reason);
      },
    });
  }
}

export function interval<MS extends number, NAME extends NonEmptyString = `$interval${MS}ms`>(
  ms: MS,
  options?: Stream.Options<void, NAME>,
): Interval<MS, NAME> {
  return new Interval(ms, options);
}
