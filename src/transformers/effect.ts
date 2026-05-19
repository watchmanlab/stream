import { Stream, Transformer } from "../core/index.ts";

const NAME = "effect";
export class Effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = effect.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: effect.Callback<VALUE, CTX>, ctx = {} as CTX) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          queueMicrotask(() => {
            batch.map((value) => callback(value, ctx));
          });
          this.batch(batch);
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}
export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = effect.Name,
>(callback: effect.Callback<VALUE, CTX>): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, VALUE, CTX, NAME>>;
export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = effect.Name,
>(
  ctx: CTX,
  callback: effect.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, VALUE, CTX, NAME>>;
export function effect<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = effect.Name,
>(
  ctxOrCallback: CTX | effect.Callback<VALUE, CTX>,
  callback?: effect.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Effect<INPUT_STREAM, VALUE, CTX, NAME>> {
  return (inputStream, name) =>
    new Effect(
      name,
      inputStream,
      callback ?? (ctxOrCallback as effect.Callback<VALUE, CTX>),
      callback ? (ctxOrCallback as CTX) : undefined,
    );
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE, CTX> = (value: VALUE, ctx: CTX) => void;
}
