import { Producer } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class Signal<VALUE, NAME extends NonEmptyString = "$signal"> extends Producer<VALUE, NAME> {
  constructor(options?: Producer.Options<VALUE, NAME>) {
    const { name, push, ...rest } = options ?? {};

    super({
      ...rest,
      name: name ?? ("$signal" as NAME),
      push(stream, value) {
        push?.(stream, value);
        stream.terminate("complete");
      },
    });
  }
}
