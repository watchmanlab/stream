import type { ScopeLinker } from "./scope-linker";
import { Stream } from "./stream";
import { NonEmptyString } from "./types";

export abstract class Transformer<INPUT extends Stream.AnyStream, VALUE, NAME extends NonEmptyString> extends Stream<
  VALUE,
  NAME
> {
  protected _input: INPUT;
  constructor(input: INPUT, options?: Transformer.Options<VALUE, NAME>) {
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
  export type Options<VALUE, NAME extends NonEmptyString> = Stream.Options<VALUE, NAME>;
  export type AnyTransformer = Transformer<Stream.AnyStream, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractInputStream<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;

  export type Traversal<T extends Stream.AnyStream> = Record<T["name"] | (`$${string}` & {}), Traversable<T>>;
  export type Traversable<T extends Stream.AnyStream> = [ExtractInputStream<T>] extends [never]
    ? T
    : Omit<T, "traversal"> & Traversal<ExtractInputStream<T>>;
}
