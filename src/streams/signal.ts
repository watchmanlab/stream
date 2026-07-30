import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class Signal<VALUE, NAME extends NonEmptyString = "$signal"> extends Stream<VALUE, NAME> {
  constructor(options?: Stream.Options<VALUE, NAME>) {
    const { name, push, ...rest } = options ?? {};

    super({
      ...rest,
      name: name ?? ("$signal" as NAME),
      push(self, value) {
        push?.(self, value);
        self.terminate("complete");
      },
    });
  }
}
