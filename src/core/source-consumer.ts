import type { Consumer } from "./consumer";
import type { Source } from "./types";

export class SourceConsumer<VALUE> {
  private _consumer: Consumer<VALUE, any>;
  private _pulling: boolean;

  constructor(
    public readonly source: Source<VALUE>,
    init: Consumer.Init<VALUE, any>,
  ) {
    this._pulling = false;

    this._consumer = source.listen({
      ...init,
      handler: (self, value) => {
        this._pulling = false;
        init.handler(self, value);
      },
      isReady: false,
    });
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
}
