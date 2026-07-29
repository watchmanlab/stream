import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Flat<
  INPUT extends AnyStream,
  DEPTH extends number = 0,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$flat",
> extends Transformer<INPUT, FlatArray<VALUE, DEPTH>, NAME> {
  constructor(input: INPUT, depth = 0 as DEPTH, options?: Stream.Options<FlatArray<VALUE, DEPTH>, NAME>) {
    options = { ...options };

    let cursor = 0;
    let values = [] as any[];

    const scalarContainer = [null];

    const inputConsumer = input.consume((self, value) => {
      if (Array.isArray(value)) {
        values = depth === 0 ? value : value.flat(depth);
        if (values.length === 0) {
          self.next();
          return;
        }
        cursor = 0;
      } else {
        scalarContainer[0] = value;
        values = scalarContainer;
        cursor = 0;
      }

      this.push(values[cursor++]);
      if (cursor === values.length) {
        self.next();
      }
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$flat" as NAME),
      next(self, consumer) {
        options.next?.(self, consumer);
        if (cursor === values.length) {
          inputConsumer.next();
        } else {
          self.push(values[cursor++]);
        }
      },
      terminate(self, reason) {
        values = [];
        scalarContainer[0] = null; // Instantly clean to prevent memory pinning leaks
        inputConsumer.terminate(reason);
        options.terminate?.(self, reason);
      },
    });
  }
}

export function flat<
  INPUT extends AnyStream,
  DEPTH extends number = 0,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$flat",
>(
  depth = 0 as DEPTH,
  options?: Stream.Options<FlatArray<VALUE, DEPTH>, NAME>,
): Transform<INPUT, Flat<INPUT, DEPTH, VALUE, NAME>> {
  return (input) => new Flat(input, depth, options);
}
