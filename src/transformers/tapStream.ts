import { Stream } from "../core/index.ts";
import { map } from "./map.ts";
import { pump } from "./pump.ts";

export function tapStream<INPUT_STREAM extends Stream.AnyStream>(
  callback: tapStream.Callack<INPUT_STREAM>,
): Stream.Transform<INPUT_STREAM, never, INPUT_STREAM> {
  return (inputStream) => {
    callback(inputStream);
    return inputStream;
  };
}

export namespace tapStream {
  export type Callack<INPUT_STREAM extends Stream.AnyStream> = (stream: INPUT_STREAM) => void;
}

new Stream([1, 2, 3])
  .pipe(map((v) => v.toFixed()))
  .pipe(tapStream((s) => s.traversal.root))
  .pipe(pump()).traversal.map.root;
