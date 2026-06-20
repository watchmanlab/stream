import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Iterator<VALUE> implements Source<VALUE> {
  private _iterator: globalThis.Iterator<VALUE, any, any>;
  constructor(iterator: globalThis.Iterator<VALUE> | (() => globalThis.Iterator<VALUE>)) {
    this._iterator = typeof iterator === "function" ? iterator() : iterator;
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
