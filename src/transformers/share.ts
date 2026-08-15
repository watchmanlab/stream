import { Stream } from "../core/stream";
import { AnySource } from "../core/types";

export function share<INPUT extends AnySource>() {
  return (input: INPUT) => new Stream({ source: input });
}
