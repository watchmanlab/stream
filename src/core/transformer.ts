import { Stream } from "./stream";
import type { AnyStream, NonEmptyString, Traversal } from "./types";

export abstract class Transformer<INPUT extends AnyStream, VALUE, NAME extends NonEmptyString> extends Stream<
  VALUE,
  NAME
> {
  protected _input: INPUT;

  constructor(input: INPUT, options?: Transformer.Options<VALUE, NAME>) {
    super(options);

    this._input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
      },
    });
  }
  get traversal(): Traversal<INPUT> {
    const { _input } = this;
    return new Proxy(
      {},
      {
        get() {
          return _input;
        },
      },
    ) as Traversal<INPUT>;
  }
}

export namespace Transformer {
  export type Options<VALUE, NAME extends NonEmptyString> = Stream.Options<VALUE, NAME>;
}
