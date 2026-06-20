import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Iterable<VALUE> implements Source<VALUE> {
  constructor(private iterable: globalThis.Iterable<VALUE> | (() => globalThis.Iterable<VALUE>)) {}
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const iterator =
      typeof this.iterable === "function" ? this.iterable()[Symbol.iterator]() : this.iterable[Symbol.iterator]();
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
