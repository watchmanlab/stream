import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, Transform } from "../core/types";

export class Map<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
> extends Transformer<INPUT, MAPPED> {
  constructor(input: INPUT, mapper: Map.Mapper<VALUE, MAPPED>, options?: Stream.Options<MAPPED>) {
    const inputConsumer = input.listen((_, value) => this.push(mapper(value)));
    super(input, {
      ...options,
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
export function map<INPUT extends AnyStream, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>, MAPPED = VALUE>(
  mapper: Map.Mapper<VALUE, MAPPED>,
  options?: Stream.Options<MAPPED>,
): Transform<INPUT, Map<INPUT, VALUE, MAPPED>> {
  return (input) => new Map(input, mapper, options);
}
export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
