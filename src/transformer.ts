import { Mitto } from "./mitto";

export abstract class Transformer<INPUT extends Mitto.AnyMitto, VALUE, NAME extends string> extends Mitto<VALUE, NAME> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    options: Mitto.Options<VALUE, NAME>,
  ) {
    let scoop: Mitto.Scoop | undefined = options.scoop;

    if (scoop instanceof Mitto) {
      scoop = { any: [input, scoop] };
    } else if (scoop) {
      if (scoop.any) {
        scoop = { any: [input, ...scoop.any] };
      } else {
        scoop = { all: [input, ...scoop.all] };
      }
    } else {
      scoop = input;
    }

    super({ ...options, name, scoop });

    this.input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
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
  export type AnyTransformer = Transformer<Mitto.AnyMitto, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractMitto<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
  export type Traversable<T extends Mitto.AnyMitto> =
    ExtractMitto<T> extends never
      ? T
      : Omit<T, "traversal"> & Record<ExtractMitto<T>["name"] | (`$${string}` & {}), Traversable<ExtractMitto<T>>>;
}
