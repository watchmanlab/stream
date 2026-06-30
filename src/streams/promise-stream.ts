import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class PromiseStream<VALUE, NAME extends NonEmptyString> extends Stream<VALUE, NAME> {
  constructor(
    public readonly promise: Promise<VALUE>,
    options?: PromiseStream.Options<VALUE, NAME>,
  ) {
    super({
      ...options,
      source: {
        listen: (handler, options) => {
          promise
            .then((value) => consumer.push(value))
            .catch((error) => consumer.abort(error))
            .finally(() => consumer.complete());

          const consumer = new Consumer<VALUE, any>(handler, options);
          return consumer;
        },
      },
    });
  }
}

export namespace PromiseStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
