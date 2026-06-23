import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class AbortSignalStream<VALUE extends void, NAME extends string = abortSignalStream.Name> extends Stream<
  void,
  NAME
> {
  constructor(
    name = abortSignalStream.NAME as NAME,
    public readonly signal: AbortSignal,

    init?: abortSignalStream.Init<void, NAME>,
  ) {
    super(name, {
      ...init,
      source: {
        listen: (init) => {
          let abortController = new AbortController();
          const consumer = new Consumer<void, any>({
            ...init,
            abort: (self, error) => {
              abortController.abort();
              init.abort?.(self, error);
            },
            complete: (self) => {
              abortController.abort();
              init.complete?.(self);
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

export function abortSignalStream<NAME extends string>(
  name: NAME,
  signal: AbortSignal,
  init?: abortSignalStream.Init<void, NAME>,
): AbortSignalStream<void, NAME>;
export function abortSignalStream<NAME extends string>(
  signal: AbortSignal,
  init?: abortSignalStream.Init<void, NAME>,
): AbortSignalStream<void, NAME>;
export function abortSignalStream<NAME extends string>(
  nameOrSignal: NAME | AbortSignal,
  signalOrInit?: AbortSignal | abortSignalStream.Init<void, NAME>,
  init?: abortSignalStream.Init<void, NAME>,
): AbortSignalStream<void, NAME> {
  return typeof nameOrSignal === "string"
    ? new AbortSignalStream(nameOrSignal, signalOrInit as AbortSignal, init)
    : new AbortSignalStream(undefined, nameOrSignal, signalOrInit as abortSignalStream.Init<void, NAME>);
}
export namespace abortSignalStream {
  export const NAME = "abortSignalStream";
  export type Name = typeof NAME;
  export type Init<VALUE, NAME extends string> = Omit<stream.Init<VALUE, NAME>, "source">;
}
