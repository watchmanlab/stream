import { Stream } from "./stream";
import type { AnyStream, NonEmptyString, TerminateReason, Traversal } from "./types";

export abstract class Transformer<INPUT extends AnyStream, VALUE, NAME extends NonEmptyString> extends Stream<
  VALUE,
  NAME
> {
  protected _input: INPUT;

  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    const $terminate = new Stream<TerminateReason>();

    super({
      ...options,
      $terminate: $terminate,
    });

    input.$terminate
      .consume((self, reason) => {
        $terminate.push(reason);
        $terminate.terminate(reason);
      })
      .next();
    options.$terminate
      ?.consume((self, reason) => {
        $terminate.push(reason);
        $terminate.terminate(reason);
      })
      .next();

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
    ) as never;
  }
}
