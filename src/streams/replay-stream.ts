import { Stream, type stream } from "../core/stream";

export class ReplayStream<VALUE, NAME extends string> extends Stream<VALUE, NAME> {
  constructor(values: [VALUE, ...VALUE[]], init?: stream.Init<VALUE, NAME>) {
    super({
      ...init,
      consumerJoin(self, consumer) {
        for (let i = 0, len = values.length; i < len; i++) {
          consumer.push(values[i]);
        }
        init?.consumerJoin?.(self, consumer);
      },
      abort(self, error) {
        values.length = 0;
        init?.abort?.(self, error);
      },
      complete(self) {
        values.length = 0;
        init?.complete?.(self);
      },
    });
  }
}
