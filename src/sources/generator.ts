import { Consumer } from "../core/consumer";
import { Source } from "../core/types";

export class Generator<VALUE> implements Source<VALUE> {
  private _generator: globalThis.Generator<VALUE, any, any>;
  constructor(functionGenerator: () => globalThis.Generator<VALUE>) {
    this._generator = functionGenerator();
  }
  listen<ERROR>(init: Consumer.Init<VALUE, ERROR>): Consumer<VALUE, ERROR> {
    return new Consumer<VALUE, ERROR>({
      ...init,
      ready: (self) => {
        const result = this._generator.next();
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
