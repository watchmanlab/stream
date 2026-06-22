import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

export class IterableStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(
    public readonly iterable: Iterable<VALUE> | (() => Iterable<VALUE>),
    init?: Omit<Stream.Init<VALUE, NAME>, "source">,
  ) {
    super({
      ...init,
      source: {
        listen: (init) => {
          const iterator = typeof iterable === "function" ? iterable()[Symbol.iterator]() : iterable[Symbol.iterator]();
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
              iterator.return?.(error);
              init.abort?.(self, error);
            },
            complete: (self) => {
              iterator.return?.(undefined);
              init.complete?.(self);
            },
          });
        },
      },
    });
  }
}
