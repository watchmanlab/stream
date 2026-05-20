import { Stream, Transformer } from "../core/index.ts";
import { pump } from "./pump.ts";

const NAME = "each";
export class Each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = each.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, callback: each.Callback<VALUE, CTX>, ctx = {} as CTX) {
    const channel = inputStream.channels.get({
      next: (batch) => {
        batch.forEach((value) => callback(value, ctx));
        this.batch(batch);
      },
    });
    super(name, inputStream, {
      next: () => channel.next(),
      return: () => channel.return(),
    });
  }
}
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = each.Name,
>(callback: each.Callback<VALUE, CTX>): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, VALUE, CTX, NAME>>;
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = each.Name,
>(
  ctx: CTX,
  callback: each.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, VALUE, CTX, NAME>>;
export function each<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  CTX = {},
  NAME extends string = each.Name,
>(
  ctxOrCallback: CTX | each.Callback<VALUE, CTX>,
  callback?: each.Callback<VALUE, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Each<INPUT_STREAM, VALUE, CTX, NAME>> {
  return (inputStream, name) =>
    new Each(
      name,
      inputStream,
      callback ?? (ctxOrCallback as each.Callback<VALUE, CTX>),
      callback ? (ctxOrCallback as CTX) : undefined,
    );
}

export namespace each {
  export type Name = typeof NAME;
  export type Callback<VALUE, CTX> = (value: VALUE, ctx: CTX) => void;
}

function test() {
  const stream = new Stream([1, 2, 3, 4])
    // .pipe(batch(2))
    .pipe(each((v) => console.log(v)))
    .pipe(pump());
}

test();
