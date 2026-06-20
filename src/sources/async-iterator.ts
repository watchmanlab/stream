import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class AsyncIterator<VALUE> implements Source<VALUE> {
  constructor(private asyncItrator: globalThis.AsyncIterator<VALUE> | (() => globalThis.AsyncIterator<VALUE>)) {}
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const iterator = typeof this.asyncItrator === "function" ? this.asyncItrator() : this.asyncItrator;
    const outputConsumer = new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        iterator.next().then((result) => {
          if (result.done) {
            self.complete();
          } else {
            self.push(result.value);
          }
        });
        init.ready?.(self);
      },
      abort: (self, error) => {
        iterator.return?.(error);
        init.abort?.(self, error);
      },
      complete: (self) => {
        iterator.return?.(undefined);
        init.complete?.(self);
      },
    });
    return outputConsumer;
  }
}
