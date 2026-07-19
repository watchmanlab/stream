import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class ReplayStream<VALUE, NAME extends NonEmptyString> extends Stream<VALUE, NAME> {
  constructor(values: [VALUE, ...VALUE[]], options?: ReplayStream.Options<VALUE, NAME>) {
    super({
      ...options,
      events: {
        ...options?.events,
        consumerJoin(self, consumer) {
          for (let i = 0, len = values.length; i < len; i++) {
            consumer.push(values[i]);
          }
          options?.events?.consumerJoin?.(self, consumer);
        },
        abort(self, error) {
          values.length = 0;
          options?.events?.abort?.(self, error);
        },
        complete(self) {
          values.length = 0;
          options?.events?.complete?.(self);
        },
      },
    });
  }
}

export namespace ReplayStream {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Stream.Options<VALUE, NAME>, "source">;
}
