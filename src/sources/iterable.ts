import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Iterable<VALUE> implements Source<VALUE> {
  private _iterator: Iterator<VALUE, any, any>;
  constructor(iterable: globalThis.Iterable<VALUE>) {
    this._iterator = iterable[Symbol.iterator]();
  }
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    return new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        const result = this._iterator.next();
        if (result.done) {
          self.complete();
        } else {
          self.push(result.value);
        }

        init.ready?.(self);
      },
    });
  }
}
