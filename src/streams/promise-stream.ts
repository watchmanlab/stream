import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class PromiseStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(
    name = promiseStream.NAME as NAME,
    public readonly promise: Promise<VALUE>,
    init?: promiseStream.Init<VALUE, NAME>,
  ) {
    super(name, {
      ...init,
      source: {
        listen: (init) => {
          promise
            .then((value) => consumer.push(value))
            .catch((error) => consumer.abort(error))
            .finally(() => consumer.complete());

          const consumer = new Consumer<VALUE, any>(init);
          return consumer;
        },
      },
    });
  }
}

export namespace promiseStream {
  export const NAME = "promiseStream";
  export type Name = typeof NAME;
  export type Init<VALUE, NAME extends string> = Omit<stream.Init<VALUE, NAME>, "source">;
}
