import { Source } from "./source";
import type { AnySource } from "./types";

export abstract class Transformer<INPUT extends AnySource, VALUE> extends Source<VALUE> {
  constructor(protected input: INPUT) {
    super();
  }
}
