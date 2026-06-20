import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Iterator<VALUE> implements Source<VALUE> {
  constructor(private iterator: globalThis.Iterator<VALUE> | (() => globalThis.Iterator<VALUE>)) {}
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const iterator = typeof this.iterator === "function" ? this.iterator() : this.iterator;
    return new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        const result = iterator.next();
        if (result.done) {
          self.complete();
        } else {
          self.push(result.value);
        }

        init.ready?.(self);
      },
      abort: (error) => iterator.return?.(error),
      complete: () => iterator.return?.(),
    });
  }
}
