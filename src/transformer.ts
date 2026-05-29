import { Mitto } from "./mitto";

export abstract class Transformer<INPUT extends Mitto.AnyMitto, VALUE, NAME extends string> extends Mitto<VALUE, NAME> {
  readonly input: INPUT;
  constructor(options: Transformer.Options<INPUT, VALUE, NAME>) {
    let scoop: Mitto.Scoop | undefined = options.scoop;
    if (scoop instanceof Mitto) {
      scoop = { any: [options.input, scoop] };
    } else if (scoop) {
      if (scoop.any) {
        scoop = { any: [options.input, ...scoop.any] };
      } else {
        scoop = { all: [options.input, ...scoop.all] };
      }
    }

    super({ ...options, scoop: options.scoop });

    this.input = options.input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return options.input;
      },
    });
  }
  get traversal(): Record<INPUT["name"] | (`$${string}` & {}), Transformer.Traversable<INPUT>> {
    const self = this;
    return new Proxy(
      {},
      {
        get() {
          return self.input;
        },
      },
    ) as never;
  }
}

export namespace Transformer {
  export type Options<INPUT extends Mitto, VALUE, NAME extends string> = Mitto.Options<VALUE, NAME> & {
    name: NAME;
    input: INPUT;
  };
  export type AnyTransformer = Transformer<Mitto.AnyMitto, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractMitto<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
  export type Traversable<T extends Mitto.AnyMitto> =
    ExtractMitto<T> extends never
      ? T
      : Omit<T, "traversal"> & Record<ExtractMitto<T>["name"] | (`$${string}` & {}), Traversable<ExtractMitto<T>>>;
}
