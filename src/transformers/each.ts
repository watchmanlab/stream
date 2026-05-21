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
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          for (let i = 0, length = batch.length; i < length; i++) {
            callback(batch[i], ctx);
          }

          this.source?.ready();
          this.batch(batch);
        },
      }),
    );
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
  const MAX = 70_000_000;
  const start = performance.now();

  const stream = new Stream([1, 2, 3]);

  stream
    .pipe(
      each((v) => {
        if (v === MAX) console.log("each1", v, "ops ->", Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(
      each((v) => {
        if (v === MAX) console.log("each2", v, "ops ->", Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(
      each((v) => {
        if (v === MAX) console.log("each3", v, "ops ->", Math.round(performance.now() - start), "ms");
      }),
    )
    .pipe(pump());

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }

  // stream.batch([11]);
  // stream.batch([22]);
  // stream.batch([33]);
}

test();
//log
// each1 70000000 ops -> 871 ms
// each2 70000000 ops -> 1005 ms
// each3 70000000 ops -> 1139 ms
