import { Stream, Transformer } from "../core/index.ts";

const NAME = "tap";
export class Tap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = tap.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: tap.Callback<VALUE, CTX>, ctx = {} as CTX) {
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
export function tap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = tap.Name,
>(callback: tap.Callback<VALUE, CTX>): Stream.Transform<INPUT_STREAM, NAME, Tap<INPUT_STREAM, VALUE, CTX, NAME>>;
export function tap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = tap.Name,
>(
  ctx: CTX,
  callback: tap.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Tap<INPUT_STREAM, VALUE, CTX, NAME>>;
export function tap<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = tap.Name,
>(
  ctxOrCallback: CTX | tap.Callback<VALUE, CTX>,
  callback?: tap.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Tap<INPUT_STREAM, VALUE, CTX, NAME>> {
  return (inputStream, name) =>
    new Tap(
      name,
      inputStream,
      callback ?? (ctxOrCallback as tap.Callback<VALUE, CTX>),
      callback ? (ctxOrCallback as CTX) : undefined,
    );
}

export namespace tap {
  export type Name = typeof NAME;
  export type Callback<VALUE, CTX> = (value: VALUE, ctx: CTX) => void;
}
