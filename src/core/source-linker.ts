import type { Consumer } from "./consumer";
import type { Closable, Source } from "./types";

export class SourceLinker<VALUE> implements Closable {
  private _consumer: Consumer<VALUE, any, any>;
  private _pulling: boolean;

  constructor(
    source: Source<VALUE>,
    handler: Consumer.Handler<VALUE, any, any>,
    options?: SourceLinker.Options<VALUE>,
  ) {
    this._pulling = false;

    this._consumer = source.listen(
      (self, value) => {
        this._pulling = false;
        handler(self, value);
      },
      {
        ...options,
        isReady: false,
      },
    );
  }
  next(error?: any): void {
    if (!this._pulling) {
      this._pulling = true;
      this._consumer.next(error);
    }
  }
  abort(error?: any): void {
    this._consumer.abort(error);
  }
  complete(): void {
    this._consumer.complete();
  }
  get pulling(): boolean {
    return this._pulling;
  }
}

export namespace SourceLinker {
  export type Options<VALUE> = Omit<Consumer.Options<VALUE, any, any>, "isReady">;
}
