import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class AsyncIterator<VALUE> implements Source<VALUE> {
  private _asyncIterator: globalThis.AsyncIterator<VALUE, any, any>;
  constructor(asyncItrator: globalThis.AsyncIterator<VALUE> | (() => globalThis.AsyncIterator<VALUE>)) {
    this._asyncIterator = typeof asyncItrator === "function" ? asyncItrator() : asyncItrator;
  }
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const outputConsumer = new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        this._asyncIterator.next().then((result) => {
          if (result.done) {
            self.complete();
          } else {
            self.push(result.value);
          }
        });
        init.ready?.(self);
      },
    });
    return outputConsumer;
  }
}
