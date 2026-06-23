import { Stream, type stream } from "../core/stream";

export class ReplayStream<VALUE, NAME extends string = replayStream.Name> extends Stream<VALUE, NAME> {
  constructor(name = replayStream.NAME as NAME, values: [VALUE, ...VALUE[]], init?: replayStream.Init<VALUE, NAME>) {
    super(name, {
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

export namespace replayStream {
  export const NAME = "replayStream";
  export type Name = typeof NAME;
  export type Init<VALUE, NAME extends string> = Omit<stream.Init<VALUE, NAME>, "source">;
}
