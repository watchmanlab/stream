import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { IterableStream } from "./iterable-stream";
import { IteratorStream } from "./iterator-stream";

export class GeneratorStream<VALUE, NAME extends string> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly functionGenerator: () => Generator<VALUE>,
    init?: Omit<Stream.Init<VALUE, NAME>, "source">,
  ) {
    super(functionGenerator, init);
  }
}
