import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class AsyncIteratorStream<VALUE, NAME extends NonEmptyString> extends Stream<VALUE, NAME> {
  constructor(
    public readonly asyncItrator: AsyncIterator<VALUE> | (() => AsyncIterator<VALUE>),
    options?: AsyncIteratorStream.Options<VALUE, NAME>,
  ) {
    super({
      ...options,
      source: {
        listen: (handler, options) => {
          const iterator = typeof asyncItrator === "function" ? asyncItrator() : asyncItrator;
          const outputConsumer = new Consumer<VALUE, any>(handler, {
            ...options,
            events: {
              ...options?.events,
              ready: (self) => {
                iterator.next().then((result) => {
                  if (result.done) {
                    self.complete();
                  } else {
                    self.push(result.value);
                  }
                });
                options?.events?.ready?.(self);
              },
              abort: (self, error) => {
                iterator.return?.(error);
                options?.events?.abort?.(self, error);
              },
              complete: (self) => {
                iterator.return?.(undefined);
                options?.events?.complete?.(self);
              },
            },
          });
          return outputConsumer;
        },
      },
    });
  }
}

export namespace AsyncIteratorStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
