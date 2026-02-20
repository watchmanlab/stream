import { Stream } from "../../streams/stream/stream-0";

export function effect<INPUT extends Stream<any>>(
  callback: (value: Stream.ValueOf<INPUT>) => void,
): Stream.Transformer<INPUT, Stream<Stream.ValueOf<INPUT>>> {
  return function (source) {
    return new Stream(async function* () {
      try {
        for await (const value of source) {
          callback(value);
          yield value;
        }
      } finally {
        return;
      }
    });
  };
}
