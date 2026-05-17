import { Stream, Transformer } from "../core/index.ts";

const NAME = "map";
class Map<
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
      inputStream.channels.get({
        next: (batch) => {
          for (let i = 0, length = batch.length; i < length; i++) {
            try {
              this.push(mapper(batch[i], ctx));
            } catch (error) {
              // this.push(error as);
            }
          }
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
  CTX = {},
  NAME extends string = map.Name,
>(
  mapper: map.Mapper<VALUE, MAPPED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Map<INPUT_STREAM, VALUE, MAPPED, CTX, NAME>>;
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
    const [_ctx, _mapper] = mapper
      ? [ctxOrMapper as CTX, mapper]
      : [undefined, ctxOrMapper as map.Mapper<VALUE, MAPPED, CTX>];
    return new Map(name, inputStream, _mapper, _ctx);
  };
}

export namespace map {
  export type Name = typeof NAME;
  export type Mapper<VALUE, MAPPED, CTX> = (value: VALUE, ctx: CTX) => MAPPED;
}
