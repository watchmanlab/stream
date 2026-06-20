import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class AsyncGenerator<VALUE> implements Source<VALUE> {
  constructor(private asyncFunctionGenerator: () => globalThis.AsyncGenerator<VALUE>) {}
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    const iterator = this.asyncFunctionGenerator();

    return new Consumer<VALUE, ERROR>({
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
      abort: (error) => iterator.return?.(error),
      complete: () => iterator.return?.(undefined),
    });
  }
}
