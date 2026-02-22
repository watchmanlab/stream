import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name, ERROR = unknown> extends Stream<VALUE, NAME, ERROR> {
  constructor(source: Stream<VALUE, any, any>, name = NAME as NAME, callback: effect.Callback<VALUE, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let result = await generator.next();

      while (!result.done) {
        try {
          const maybeError = await callback(result.value);

          if (maybeError) {
            result = await generator.next(maybeError);
          } else {
            const downstreamError = yield result.value;
            result = await generator.next(downstreamError);
          }
        } catch (error) {
          result = await generator.next(new Stream.Error(error as ERROR, result.value as VALUE, name));
        }
      }
    });
  }
}
export function effect<VALUE, NAME extends string = effect.Name, ERROR = unknown>(
  callback: effect.Callback<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any, any>, Effect<VALUE, NAME, ERROR>> {
  return (_, source, name) => new Effect(source, name, callback);
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
  ) => Stream.MaybeError<VALUE, ERROR> | Promise<Stream.MaybeError<VALUE, ERROR>>;
}
