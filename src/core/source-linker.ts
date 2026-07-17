import type { Consumer } from "./consumer";
import type { Source } from "./types";

export class SourceLinker<VALUE> {
  #consumer: Consumer<VALUE, any>;
  #pulling: boolean;

  constructor(source: Source<VALUE>, handler: Consumer.Handler<VALUE, any>, options?: SourceLinker.Options<VALUE>) {
    this.#pulling = false;

    this.#consumer = source.listen(
      (self, value) => {
        this.#pulling = false;
        handler(self, value);
      },
      {
        ...options,
        ready: false,
      },
    );
  }
  next(): void {
    if (!this.#pulling) {
      this.#pulling = true;
      this.#consumer.next();
    }
  }
  abort(): void {
    this.#consumer.abort();
  }
  complete(): void {
    this.#consumer.complete();
  }
}

export namespace SourceLinker {
  export type Options<VALUE> = Omit<Consumer.Options<VALUE, any>, "ready">;
}
