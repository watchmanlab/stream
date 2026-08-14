import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Flat<
  INPUT extends Producer<Array<any>, any>,
  DEPTH extends number = 0,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$flat",
> extends Transformer<INPUT, FlatArray<VALUE, DEPTH>, NAME> {
  constructor(input: INPUT, depth = 0 as DEPTH, options?: Producer.Options<FlatArray<VALUE, DEPTH>, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    let cursor = 0;
    let values = [] as any[];

    const inputConsumer = input.consume((self, value) => {
      if (!value.length) {
        self.next();
        return;
      }
      values = depth === 0 ? value : value.flat(depth);
      cursor = 0;

      this.push(values[cursor++]);
    });

    super(input, {
      ...rest,
      name: name ?? ("$flat" as NAME),
      next(self, consumer) {
        if (cursor === values.length) {
          inputConsumer.next();
        } else {
          self.push(values[cursor++]);
        }
        next?.(self, consumer);
      },
      terminate(self, reason) {
        values = [];
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function flat<
  INPUT extends Producer<Array<any>, any>,
  DEPTH extends number = 0,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$flat",
>(
  depth = 0 as DEPTH,
  options?: Producer.Options<FlatArray<VALUE, DEPTH>, NAME>,
): Transform<INPUT, Flat<INPUT, DEPTH, VALUE, NAME>> {
  return (input) => new Flat(input, depth, options);
}
