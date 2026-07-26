import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class Timeout<MS extends number, NAME extends NonEmptyString = `$interval${MS}ms`> extends Stream<void, NAME> {
  constructor(ms: MS, options?: Stream.Options<void, NAME>) {
    const timer = setTimeout(() => {
      this.push();
      this.terminate("complete");
    }, ms);
    super({
      ...options,
      name: options?.name ?? (`$timeout${ms}ms` as NAME),
      terminate(self, reason) {
        clearTimeout(timer);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function timeout<MS extends number, NAME extends NonEmptyString = `$interval${MS}ms`>(
  ms: MS,
  options?: Stream.Options<void, NAME>,
): Timeout<MS, NAME> {
  return new Timeout(ms, options);
}
