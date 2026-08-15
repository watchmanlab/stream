import { DefaultSizedQueue } from "../core/default-sized-queue";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class ReplayLatest<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
> extends Transformer<INPUT, VALUE, NAME> {
  private _queue: DefaultSizedQueue<VALUE>;
  constructor(input: INPUT, size: number, options?: Stream.Options<VALUE, NAME>) {
    const { name, next, consumerJoin, terminate, ...rest } = options ?? {};

    const inputConsumer = input.consume((_, value) => {
      this.push(value);
      this._queue.enqueue(value);
    });

    super(input, {
      ...rest,
      name: name ?? ("$replayLatest" as NAME),
      next(self, consumer) {
        inputConsumer.next();
        next?.(self, consumer);
      },
      consumerJoin: (self, consumer) => {
        for (const value of this._queue) {
          consumer.push(value);
        }

        consumerJoin?.(self, consumer);
      },
      terminate: (self, reason) => {
        this._queue.clear();
        inputConsumer.terminate(reason);
        terminate?.(self, reason);
      },
    });

    this._queue = new DefaultSizedQueue(size);
  }
}

export function replayLatest<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
>(last: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, ReplayLatest<INPUT, VALUE, NAME>> {
  return (input) => new ReplayLatest(input, last, options);
}
