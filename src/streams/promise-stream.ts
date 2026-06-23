import { Consumer } from "../core/consumer";
import { Stream, type stream } from "../core/stream";

export class PromiseStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(
    public readonly promise: Promise<VALUE>,
    init?: Omit<stream.Init<VALUE, NAME>, "source">,
  ) {
    super({
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
