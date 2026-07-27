import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Replay<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replay",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, values: [VALUE, ...VALUE[]], options?: Stream.Options<VALUE, NAME>) {
    const inputConsumer = input.consume((self, value) => {
      this.push(value);
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$replay" as NAME),
      next(self, consumer) {
        options?.next?.(self, consumer);
        inputConsumer.next();
      },
      consumerJoin(self, consumer) {
        for (let i = 0, len = values.length; i < len; i++) {
          consumer.push(values[i]);
        }
        options?.consumerJoin?.(self, consumer);
      },
      terminate(self, reason) {
        values.length = 0;
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });
  }
}

export function replay<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replay",
>(values: [VALUE, ...VALUE[]], options?: Stream.Options<VALUE, NAME>): Transform<INPUT, Replay<INPUT, VALUE, NAME>> {
  return (input) => new Replay(input, values, options);
}
