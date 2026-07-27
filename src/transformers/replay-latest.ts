import { SizedLinkedListQueue } from "../core/sized-linked-list-queue";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class ReplayLatest<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
> extends Transformer<INPUT, VALUE, NAME> {
  private _queue: SizedLinkedListQueue<VALUE>;
  constructor(input: INPUT, size: number, options?: Stream.Options<VALUE, NAME>) {
    const inputConsumer = input.consume((self, value) => {
      this.push(value);
      this._queue.enqueue(value);
    });

    super(input, {
      ...options,
      name: options?.name ?? ("$replayLatest" as NAME),
      next(self, consumer) {
        options?.next?.(self, consumer);
        inputConsumer.next();
      },
      consumerJoin: (self, consumer) => {
        for (const value of this._queue) {
          consumer.push(value);
        }

        options?.consumerJoin?.(self, consumer);
      },
      terminate: (self, reason) => {
        this._queue.clear();
        inputConsumer.terminate(reason);
        options?.terminate?.(self, reason);
      },
    });

    this._queue = new SizedLinkedListQueue(size);
  }
}

export function replayLatest<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
>(last: number, options?: Stream.Options<VALUE, NAME>): Transform<INPUT, ReplayLatest<INPUT, VALUE, NAME>> {
  return (input) => new ReplayLatest(input, last, options);
}
