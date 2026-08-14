import { Producer } from "../core/producer";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Replay<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replay",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, values: [VALUE, ...VALUE[]], options?: Producer.Options<VALUE, NAME>) {
    const { name, next, consumerJoin, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      this.push(value);
    });

    super(input, {
      ...rest,
      name: name ?? ("$replay" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        next?.(self, consumer);
      },
      consumerJoin(self, consumer) {
        for (let i = 0, len = values.length; i < len; i++) {
          consumer.push(values[i]);
        }
        consumerJoin?.(self, consumer);
      },
      terminate(self, reason) {
        values.length = 0;
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function replay<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replay",
>(values: [VALUE, ...VALUE[]], options?: Producer.Options<VALUE, NAME>): Transform<INPUT, Replay<INPUT, VALUE, NAME>> {
  return (input) => new Replay(input, values, options);
}
