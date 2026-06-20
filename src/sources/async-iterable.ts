import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class AsyncIterable<VALUE> implements Source<VALUE> {
  private _asyncIterator: AsyncIterator<VALUE, any, any>;
  constructor(asyncItrable: globalThis.AsyncIterable<VALUE>) {
    this._asyncIterator = asyncItrable[Symbol.asyncIterator]();
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
