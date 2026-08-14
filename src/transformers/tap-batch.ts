import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class TapBatch<
  INPUT extends Producer<Array<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$tapBatch",
> extends Transformer<INPUT, VALUE[], NAME> {
  constructor(input: INPUT, fn: (value: VALUE, INPUT: INPUT) => void, options?: Producer.Options<VALUE[], NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, values) => {
      for (let i = 0, len = values.length; i < len; i++) {
        fn(values[i], input);
      }
      this.push(values);
    });

    super(input, {
      ...rest,
      name: name ?? ("$tapBatch" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        next?.(self, consumer);
      },
      terminate(self, reason) {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function tapBatch<
  INPUT extends Producer<Array<any>, any>,
  VALUE extends ExtractValue<INPUT, 1> = ExtractValue<INPUT, 1>,
  NAME extends NonEmptyString = "$tapBatch",
>(
  fn: (value: VALUE, INPUT: INPUT) => void,
  options?: Producer.Options<VALUE[], NAME>,
): Transform<INPUT, TapBatch<INPUT, VALUE, NAME>> {
  return (input) => new TapBatch(input, fn, options);
}
