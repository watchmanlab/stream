import { Stream } from "./stream";
import type { AnyStream, NonEmptyString, TerminateReason, Traversal } from "./types";

export abstract class Transformer<INPUT extends AnyStream, VALUE, NAME extends NonEmptyString> extends Stream<
  VALUE,
  NAME
> {
  protected _input: INPUT;

  constructor(input: INPUT, options?: Stream.Options<VALUE, NAME>) {
    options = { ...options };

    super({
      ...options,
      $terminate: options.$terminate ?? input.$terminate,
    });

    this._input = input;

    Object.defineProperty(this, input.name, {
      get() {
        return input;
      },
    });
  }
  get traversal(): Traversal<INPUT> {
    const { _input: input } = this;

    return Object.defineProperty({}, input.name, {
      get() {
        return input;
      },
    }) as never;
  }
}

export namespace Transformer {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
