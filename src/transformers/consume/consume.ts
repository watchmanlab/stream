import { Stream } from "../../streams/stream/stream";
import { signal } from "../signal";

export function consume<VALUE>(
  callback: (value: VALUE, signal: signal.CapableStream) => void,
  signal: signal.CapableStream,
): Stream.Transformer<Stream<VALUE>> {
  return function (source) {
    const generator = source[Symbol.asyncIterator]();

    signal.next().then(() => generator.return());

    (async () => {
      for await (const value of generator) {
        if (signal.signal.fired) break;

        callback(value, signal);
      }
    })();
    return new Stream(source);
  };
}
