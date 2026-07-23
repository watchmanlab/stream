import type { ScopeBinder } from "./scope-binder";
import { Stream } from "./stream";
import type { AnyStream, NonEmptyString, Traversal } from "./types";

export abstract class Transformer<INPUT extends AnyStream, VALUE, NAME extends NonEmptyString> extends Stream<
  VALUE,
  NAME
> {
  protected _input: INPUT;

  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    let scope: ScopeBinder.Scope | undefined = options?.scope;
    if (scope) {
      if (scope.any) {
        scope = { any: [input, ...scope.any] };
      } else {
        scope = { all: [input, ...scope.all] };
      }
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
  get traversal(): Traversal<INPUT> {
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
