import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class IteratorStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(
    public readonly iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
    init?: Omit<stream.Init<VALUE, NAME>, "source">,
  ) {
    super({
      ...init,
      source: {
        listen: (init) => {
          const iter = typeof iterator === "function" ? iterator() : iterator;
          return new Consumer<VALUE, any>({
            ...init,
            ready: (self) => {
              const result = iter.next();
              if (result.done) {
                self.complete();
              } else {
                self.push(result.value);
              }

              init.ready?.(self);
            },
            abort: (self, error) => {
              iter.return?.(error);
              init.abort?.(self, error);
            },
            complete: (self) => {
              iter.return?.(undefined);
              init.complete?.(self);
            },
          });
        },
      },
    });
  }
}
