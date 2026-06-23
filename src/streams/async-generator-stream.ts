import type { stream } from "../core/stream";
import { AsyncIteratorStream } from "./async-iterator-stream";

export class AsyncGeneratorStream<VALUE, NAME extends string> extends AsyncIteratorStream<VALUE, NAME> {
  constructor(
    public readonly asyncFunctionGenerator: () => AsyncGenerator<VALUE>,
    init?: Omit<stream.Init<VALUE, NAME>, "source">,
  ) {
    super(asyncFunctionGenerator, init);
  }
}
