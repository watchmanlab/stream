import type { ScopeBinder } from "./scope-binder";
import { Stream } from "./stream";

export abstract class Transformer<INPUT extends Stream.AnyStream, VALUE, NAME extends string> extends Stream<
  VALUE,
  NAME
> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    init: Omit<Stream.Init<VALUE, NAME>, "name">,
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

    super({ ...init, name, scope });

    this.input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
      },
    });
  }
  get traversal(): Transformer.Traversal<INPUT> {
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
  export type Init<VALUE, NAME extends string> = Omit<Stream.Init<VALUE, NAME>, "name">;
  export type AnyTransformer = Transformer<Stream.AnyStream, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;

  export type Traversal<T extends Stream.AnyStream> = Record<
    T["name"] | (`$${string}` & {}),
    Transformer.Traversable<T>
  >;
  export type Traversable<T extends Stream.AnyStream> = [ExtractInputStream<T>] extends [never]
    ? T
    : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;
}
