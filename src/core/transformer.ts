import type { ScopeBinder } from "./scope-binder";
import { Stream, stream } from "./stream";

export abstract class Transformer<INPUT extends stream.AnyStream, VALUE, NAME extends string> extends Stream<
  VALUE,
  NAME
> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    init: transformer.Init<VALUE, NAME>,
  ) {
    let scope: ScopeBinder.Scope | undefined = init.scope;

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

    super(name, { ...init, scope });

    this.input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
      },
    });
  }
  get traversal(): transformer.Traversal<INPUT> {
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

export namespace transformer {
  export type Init<VALUE, NAME extends string> = Omit<stream.Init<VALUE, NAME>, "name">;
  export type AnyTransformer = Transformer<stream.AnyStream, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;

  export type Traversal<T extends stream.AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
  export type Traversable<T extends stream.AnyStream> = [ExtractInputStream<T>] extends [never]
    ? T
    : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;
}
