import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Generator<VALUE> implements Source<VALUE> {
  constructor(private functionGenerator: () => globalThis.Generator<VALUE>) {}
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const iterator = this.functionGenerator();
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
      complete: () => iterator.return?.(undefined),
    });
  }
}
