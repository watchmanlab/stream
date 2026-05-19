import { Stream, Transformer } from "../core/index.ts";

const NAME = "flatMap";
export class FlatMap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = flatMap.Name,
> extends Transformer<INPUT_STREAM, MAPPED, NAME> {
  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    mapper: flatMap.Mapper<VALUE, MAPPED, CTX>,
    ctx = {} as CTX,
  ) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          this.batch(batch.flatMap((value) => mapper(value, ctx)));
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}
export function flatMap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = flatMap.Name,
>(
  mapper: flatMap.Mapper<VALUE, MAPPED, {}>,
): Stream.Transform<INPUT_STREAM, NAME, FlatMap<INPUT_STREAM, VALUE, MAPPED, {}, NAME>>;
export function flatMap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = flatMap.Name,
>(
  ctx: CTX,
  mapper: flatMap.Mapper<VALUE, MAPPED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, FlatMap<INPUT_STREAM, VALUE, MAPPED, CTX, NAME>>;
export function flatMap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = flatMap.Name,
>(
  ctxOrMapper: flatMap.Mapper<VALUE, MAPPED, CTX> | CTX,
  mapper?: flatMap.Mapper<VALUE, MAPPED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, FlatMap<INPUT_STREAM, VALUE, MAPPED, CTX, NAME>> {
  return (inputStream, name) => {
    return new FlatMap(
      name,
      inputStream,
      mapper ?? (ctxOrMapper as flatMap.Mapper<VALUE, MAPPED, CTX>),
      mapper ? (ctxOrMapper as CTX) : undefined,
    );
  };
}

export namespace flatMap {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, CTX> = (value: VALUE, ctx: CTX) => MAPPED | readonly MAPPED[];
}
