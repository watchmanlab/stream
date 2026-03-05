import { Stream } from "../../streams";
import { each } from "../each";
import { pump } from "../pump";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _errors?: Stream<effect.Error<VALUE, NAME, ERROR>, `${NAME}-errors`>;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();

      let next = await generator.next();

      try {
        while (!next.done) {
          (async () => {
            try {
              const result = await callback(next.value, self);

              if (Stream.Result.isErr(result)) {
                self._errors?.push({ type: "expected", error: result.value, self });
              }
            } catch (error: any) {
              if (error instanceof Stream.Result.Err) {
                self._errors?.push({ type: "expected", error: error.value, self });
              } else {
                self._errors?.push({ type: "unexpected", error, self });
              }
            }
          })();

          const feedback = yield next.value;
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });

    const self = this;
  }

  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}-errors` as never);
    return this._errors;
  }
}
export function effect<VALUE, NAME extends string = effect.Name, ERROR = unknown>(
  callback: effect.Callback<VALUE, NAME, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Effect<VALUE, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}
export namespace effect {
  export type Name = typeof NAME;

  export type Callback<VALUE, NAME extends string, ERROR> = (
    value: VALUE,
    self: Effect<VALUE, NAME, ERROR>,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;
  export type Error<VALUE, NAME extends string, ERROR> =
    | { type: "expected"; error: ERROR; self: Effect<VALUE, NAME, ERROR> }
    | { type: "unexpected"; error: unknown; self: Effect<VALUE, NAME, ERROR> };
}

const MAX = 1_00_000;
let count = 0;
const stream = new Stream<number>()
  .pipe(
    each((v) => {
      count++;
      if (v === MAX) {
        console.timeEnd("test");
        console.log(count);
      }
    }),
  )
  .pipe(pump());

console.time("test");

for (let i = 0; i <= MAX; i++) {
  await stream.each.root.push(i);
}
