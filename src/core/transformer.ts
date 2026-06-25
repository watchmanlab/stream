import type { ScopeLinker } from "./scope-linker";
import { Stream } from "./stream";
import { Named } from "./types";

export abstract class Transformer<
  INPUT extends Stream.AnyStream,
  VALUE,
  NAME extends string,
  SELF extends Named<NAME>,
> extends Stream<VALUE, NAME, SELF> {
  protected _input: INPUT;
  constructor(input: INPUT, options?: Transformer.Options<VALUE, NAME, SELF>) {
    let scope: ScopeLinker.Scope | undefined = options?.scope;

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

    super({ ...options, scope });

    this._input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
      },
    });
  }
  get traversal(): Transformer.Traversal<INPUT> {
    const input = this._input;
    return new Proxy(
      {},
      {
        get() {
          return input;
        },
      },
    ) as never;
  }
}

export namespace Transformer {
  export type Options<VALUE, NAME extends string, SELF> = Stream.Options<VALUE, NAME, SELF>;
  export type AnyTransformer = Transformer<Stream.AnyStream, any, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any, any> ? INPUT : never;

  export type Traversal<T extends Stream.AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
  export type Traversable<T extends Stream.AnyStream> = [ExtractInputStream<T>] extends [never]
    ? T
    : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;
}
