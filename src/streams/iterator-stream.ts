import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class IteratorStream<VALUE, NAME extends NonEmptyString> extends Stream<VALUE, NAME> {
  constructor(
    public readonly iterator: Iterator<VALUE> | (() => Iterator<VALUE>),
    options?: IteratorStream.Options<VALUE, NAME>,
  ) {
    super({
      ...options,
      source: {
        listen: (handler, options) => {
          const iter = typeof iterator === "function" ? iterator() : iterator;
          return new Consumer<VALUE, any>(handler, {
            ...options,
            events: {
              ...options?.events,
              ready: (self) => {
                const result = iter.next();
                if (result.done) {
                  self.complete();
                } else {
                  self.push(result.value);
                }

                options?.events?.ready?.(self);
              },
              abort: (self, error) => {
                iter.return?.(error);
                options?.events?.abort?.(self, error);
              },
              complete: (self) => {
                iter.return?.(undefined);
                options?.events?.complete?.(self);
              },
            },
          });
        },
      },
    });
  }
}

export namespace IteratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
