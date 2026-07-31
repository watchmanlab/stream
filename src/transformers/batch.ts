import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, FixedArray, NonEmptyString, TerminateReason, Transform } from "../core/types";

export class Batch<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$batch",
> extends Transformer<INPUT, FixedArray<VALUE, SIZE>, NAME> {
  private _batch = [] as any[];
  constructor(input: INPUT, size: SIZE, options?: Stream.Options<FixedArray<VALUE, SIZE>, NAME>) {
    const { name, next, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((self, value) => {
      this._batch.push(value);
      if (this._batch.length < size) {
        self.next();
      } else {
        const array = [...this._batch];
        this._batch.length = 0;
        this.push(array as FixedArray<VALUE, SIZE>);
      }
    });

    super(input, {
      ...rest,
      name: name ?? ("$batch" as NAME),
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
  override terminate(reason: TerminateReason): this {
    if (reason === "complete") {
      const array = [...this._batch];
      this._batch.length = 0;
      this.push(array as FixedArray<VALUE, SIZE>);
    }
    return super.terminate(reason);
  }
}

export function batch<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$batch",
>(
  size: SIZE,
  options?: Stream.Options<FixedArray<VALUE, SIZE>, NAME>,
): Transform<INPUT, Batch<INPUT, SIZE, VALUE, NAME>> {
  return (input) => new Batch(input, size, options);
}
