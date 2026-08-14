import { AnySource } from "../core/types";

export function share<INPUT extends AnySource>() {
  return (input: INPUT) => input.producer;
}
