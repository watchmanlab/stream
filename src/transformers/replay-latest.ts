import { SizedLinkedListQueue } from "../core/sized-linked-list-queue";
import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class ReplayLatest<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
> extends Transformer<INPUT, VALUE, NAME> {
  private _queue: SizedLinkedListQueue<VALUE>;
  constructor(input: INPUT, size: number, options?: Producer.Options<VALUE, NAME>) {
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

    this._queue = new SizedLinkedListQueue(size);
  }
}

export function replayLatest<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$replayLatest",
>(last: number, options?: Producer.Options<VALUE, NAME>): Transform<INPUT, ReplayLatest<INPUT, VALUE, NAME>> {
  return (input) => new ReplayLatest(input, last, options);
}
