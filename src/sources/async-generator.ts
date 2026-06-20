import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class AsyncGenerator<VALUE, ERROR = never> implements Source<VALUE> {
  private _asyncGenerator: globalThis.AsyncGenerator<VALUE, ERROR, any>;
  constructor(asyncFunctionGenerator: () => globalThis.AsyncGenerator<VALUE>) {
    this._asyncGenerator = asyncFunctionGenerator();
  }
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    return new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        this._asyncGenerator.next().then((result) => {
          if (result.done) {
            self.complete();
          } else {
            self.push(result.value);
          }
        });
        init.ready?.(self);
      },
    });
  }
}
