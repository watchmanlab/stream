import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Map$<
  INPUT extends Stream<AnyStream, any>,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$map$",
> extends Transformer<INPUT, Stream<MAPPED>, NAME> {
  constructor(input: INPUT, mapper: Map$.Mapper<VALUE, MAPPED>, options?: Stream.Options<Stream<MAPPED>, NAME>) {
    const inputConsumer = input.consume((_, value) => {
      const stream = new Stream<MAPPED>();
      this.push(stream);
      value
        .consume(
          (self, value) => {
            stream.push(mapper(value));
            self.next();
          },
          {
            terminate: (self, reason) => {
              stream.terminate(reason);
            },
          },
        )
        .next();
    });
    super(input, {
      ...options,
      name: options?.name ?? ("$map$" as NAME),

      next(stream, consumer) {
        inputConsumer.next();
        options?.next?.(stream, consumer);
      },
      terminate(stream, reason) {
        inputConsumer.terminate(reason);
        options?.terminate?.(stream, reason);
      },
    });
  }
}
export function map$<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<ExtractValue<INPUT>> = ExtractValue<ExtractValue<INPUT>>,
  MAPPED = VALUE,
  NAME extends NonEmptyString = "$map$",
>(
  mapper: Map$.Mapper<VALUE, MAPPED>,
  options?: Stream.Options<Stream<MAPPED>, NAME>,
): Transform<INPUT, Map$<INPUT, VALUE, MAPPED, NAME>> {
  return (input) => new Map$(input, mapper, options);
}
export namespace Map$ {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
