import type { stream } from "../core/stream";
import { IteratorStream } from "./iterator-stream";

export class GeneratorStream<VALUE, NAME extends string> extends IteratorStream<VALUE, NAME> {
  constructor(
    public readonly functionGenerator: () => Generator<VALUE>,
    init?: Omit<stream.Init<VALUE, NAME>, "source">,
  ) {
    super(functionGenerator, init);
  }
}
