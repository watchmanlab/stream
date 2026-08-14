import { Producer } from "../core/producer";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, FixedArray, NonEmptyString, TerminateReason, Transform } from "../core/types";
import { Signal } from "../streams/signal";

export class Batch<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$batch",
> extends Transformer<INPUT, VALUE[], NAME> {
  constructor(input: INPUT, size: number, options?: Producer.Options<VALUE[], NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const $terminate = new Signal<TerminateReason>();

    const batch = [] as any[];

    const inputConsumer = input.consume(
      (self, value) => {
        batch.push(value);
        if (batch.length < size) {
          self.next();
        } else {
          const array = [...batch];
          batch.length = 0;
          this.push(array);
          array.length = 0;
        }
      },
      {
        terminate: (_, reason) => {
          if (reason === "complete") {
            const array = [...batch];
            batch.length = 0;
            this.push(array);
            array.length = 0;
          }
          $terminate.push(reason);
        },
      },
    );

    super(input, {
      ...rest,
      name: name ?? ("$batch" as NAME),
      $terminate,
      next(self, consumer) {
        inputConsumer.next();
        next?.(self, consumer);
      },
      terminate: (self, reason) => {
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });
  }
}

export function batch<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$batch",
>(size: number, options?: Producer.Options<VALUE[], NAME>): Transform<INPUT, Batch<INPUT, VALUE, NAME>> {
  return (input) => new Batch(input, size, options);
}
