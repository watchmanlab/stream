import type { Consumer } from "./consumer";
import type { Closable, Source } from "./types";

export class SourceLinker<VALUE> implements Closable {
  private _consumer: Consumer<VALUE, any>;
  private _pulling: boolean;

  constructor(source: Source<VALUE>, handler: Consumer.Handler<VALUE, any>, options?: SourceLinker.Options<VALUE>) {
    this._pulling = false;

    this._consumer = source.listen(
      (self, value) => {
        this._pulling = false;
        handler(self, value);
      },
      {
        ...options,
        ready: false,
      },
    );
  }
  next(): void {
    if (!this._pulling) {
      this._pulling = true;
      this._consumer.next();
    }
  }
  abort(): void {
    this._consumer.abort();
  }
  complete(): void {
    this._consumer.complete();
  }
  get pulling(): boolean {
    return this._pulling;
  }
}

export namespace SourceLinker {
  export type Options<VALUE> = Omit<Consumer.Options<VALUE, any>, "ready">;
}
