import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class AbortSignalStream<VALUE extends void, NAME extends NonEmptyString = AbortSignalStream.Name> extends Stream<
  void,
  NAME
> {
  constructor(
    public readonly signal: AbortSignal,
    options?: AbortSignalStream.Options<void, NAME>,
  ) {
    super({
      ...options,
      name: options?.name ?? (AbortSignalStream.NAME as NAME),
      source: {
        listen: (handler, options) => {
          let abortController = new AbortController();
          const consumer = new Consumer<void, any>(handler, {
            ...options,
            events: {
              ...options?.events,
              abort: (self, error) => {
                abortController.abort();
                options?.events?.abort?.(self, error);
              },
              complete: (self) => {
                abortController.abort();
                options?.events?.complete?.(self);
              },
            },
          });

          if (signal.aborted) {
            consumer.complete();
            return consumer;
          }
          signal.addEventListener(
            "abort",
            () => {
              consumer.push();
              consumer.complete();
            },
            {
              signal: abortController.signal,
            },
          );

          return consumer;
        },
      },
    });
  }
}

export namespace AbortSignalStream {
  export const NAME = "abortSignalStream";
  export type Name = typeof NAME;
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
