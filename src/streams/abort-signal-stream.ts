import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class AbortSignalStream<VALUE extends void, NAME extends string> extends Stream<void, NAME> {
  constructor(
    public readonly signal: AbortSignal,

    init?: Omit<stream.Init<void, NAME>, "source">,
  ) {
    super({
      ...init,

      source: {
        listen: (init) => {
          let abortController = new AbortController();
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

          return consumer;
        },
      },
    });
  }
}
