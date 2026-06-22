import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

export class GeneratorStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(
    public readonly functionGenerator: () => Generator<VALUE>,
    init?: Omit<Stream.Init<VALUE, NAME>, "source">,
  ) {
    super({
      ...init,
      source: {
        listen: (init) => {
          const iterator = this.functionGenerator();
          return new Consumer<VALUE, any>({
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
            abort: (self, error) => {
              iterator.return(error);
              init.abort?.(self, error);
            },
            complete: (self) => {
              iterator.return(undefined);
              init.complete?.(self);
            },
          });
        },
      },
    });
  }
}
