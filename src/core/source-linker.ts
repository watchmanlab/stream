import type { Consumer } from "./consumer";
import type { Source } from "./types";

export class SourceLinker<VALUE> {
  private _consumer: Consumer<VALUE, any>;
  private _pulling: boolean;

  constructor(
    public readonly source: Source<VALUE>,
    public readonly handler: Consumer.Handler<VALUE, ErrnoException>,
    options?: Consumer.Options<VALUE, any>,
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
  next(): void {
    if (!this._pulling) {
      this._pulling = true;
      this._consumer.next();
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
