import { Stream } from "./stream0";

export abstract class Transformer<INPUT extends Stream.AnyStream, VALUE, NAME extends string> extends Stream<
  VALUE,
  NAME
> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    options: Stream.Options<VALUE, NAME>,
  ) {
    let scope: Stream.Scoop | undefined = options.scope;

    if (scope instanceof Stream) {
      scope = { any: [input, scope] };
    } else if (scope) {
      if (scope.any) {
        scope = { any: [input, ...scope.any] };
      } else {
        scope = { all: [input, ...scope.all] };
      }
    } else {
      scope = input;
    }

    super({ ...options, name, scope });

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
  export type AnyTransformer = Transformer<Stream.AnyStream, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractMitto<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
  export type Traversable<T extends Stream.AnyStream> =
    ExtractMitto<T> extends never
      ? T
      : Omit<T, "traversal"> & Record<ExtractMitto<T>["name"] | (`$${string}` & {}), Traversable<ExtractMitto<T>>>;
}
