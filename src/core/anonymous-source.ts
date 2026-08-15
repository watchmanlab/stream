import { Consumer } from "./consumer";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { ConsumerSet, Queue } from "./types";

export class AnonymousSource<VALUE> extends Source<VALUE> {
  private _options?: AnonymousSource.Options<VALUE>;
  private _consumerSet: ConsumerSet<VALUE>;

  constructor(options?: AnonymousSource.Options<VALUE>) {
    super();
    this._options = { ...options };
    this._consumerSet = this._options.consumerSetFactory?.() ?? new DefaultConsumerSet();
  }
  push(value: VALUE) {
    this._consumerSet.push(value);
    return this;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const { queueFactory, terminate, ...rest } = options ?? {};
    const consumer = new Consumer(handler, {
      ...rest,
      queueFactory: queueFactory ?? this._options?.consumerQueueFactory,
      terminate(consumer, reason) {
        deleteConsumer();
        terminate?.(consumer, reason);
      },
    });

    const deleteConsumer = this._consumerSet.add(consumer);

    return consumer;
  }
}

export namespace AnonymousSource {
  export type Options<VALUE> = {
    consumerSetFactory?: () => ConsumerSet<VALUE>;
    consumerQueueFactory?: () => Queue<VALUE>;
  };
}
