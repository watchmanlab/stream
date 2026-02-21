import { Stream } from "../../streams";

const NAME = "effect";

export class Effect<VALUE, NAME extends string = effect.Name> extends Stream<VALUE, NAME> {
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback: effect.Callback<VALUE>) {
    super(name, async function* () {
      for await (const value of source) {
        await callback(value);
        yield value;
      }
    });
  }
}
export function effect<VALUE, NAME extends string = effect.Name>(
  callback: effect.Callback<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Effect<VALUE, NAME>> {
  return (_, source, name) => new Effect(source, name, callback);
}

export namespace effect {
  export type Name = typeof NAME;
  export type Callback<VALUE> = (value: VALUE) => void | Promise<void>;
}
