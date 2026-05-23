import { Stream, Transformer } from "../core/index.ts";

const NAME = "map";
export class Map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = map.Name,
> extends Transformer<INPUT_STREAM, MAPPED, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, mapper: map.Mapper<VALUE, MAPPED, CTX>, ctx = {} as CTX) {
    super(
      name,
      inputStream,
      inputStream.consumers.get({
        next: (batch) => {
          this.batch(batch.map((value) => mapper(value, ctx)));
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, {}>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, {}, NAME>>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = map.Name,
>(
  ctx: CTX,
  mapper: map.Mapper<VALUE, MAPPED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, CTX, NAME>>;
export function map<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  MAPPED = VALUE,
  CTX = {},
  NAME extends string = map.Name,
>(
  ctxOrMapper: map.Mapper<VALUE, MAPPED, CTX> | CTX,
  mapper?: map.Mapper<VALUE, MAPPED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, CTX, NAME>> {
  return (inputStream, name) => {
    return new Map(
      name,
      inputStream,
      mapper ?? (ctxOrMapper as map.Mapper<VALUE, MAPPED, CTX>),
      mapper ? (ctxOrMapper as CTX) : undefined,
    );
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, CTX> = (value: VALUE, ctx: CTX) => MAPPED;
}
