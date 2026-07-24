import { Stream } from "./stream";
import type { AnyStream } from "./types";

export abstract class Transformer<INPUT extends AnyStream, VALUE> extends Stream<VALUE> {
  private _input: INPUT;

  constructor(input: INPUT, options?: Stream.Options<VALUE>) {
    super(options);

    this._input = input;
  }
}
