import { Consumer } from "./consumer";
import { NonEmptyString, Queue, Consumable, TerminateReason, Terminable, ConsumerSet, AnySource } from "./types";

import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { DefaultQueue } from "./default-queue";
import { SourceProxy } from "./source-proxy";

export class Producer<VALUE>
  extends Source<VALUE>
  implements Consumable<VALUE>, Terminable, Disposable, AsyncIterable<VALUE>
{
  private _options?: Producer.Options<VALUE>;
  private _consumerSet?: ConsumerSet<VALUE>;
  private _status: Producer.Status;
  private _pulling: boolean;
  private _sourceProxy?: Source<VALUE>;
  private _initCleanup?: (reason: TerminateReason) => void;
  private _sourceConsumer?: Consumer<VALUE>;
  private _signalConsumer?: Consumer<TerminateReason>;

  constructor(options?: Producer.Options<VALUE>) {
    super();
    this._options = { ...options };
    this._status = "active";
    this._pulling = false;

    this._signalConsumer = this._options?.signal?.consume((_, reason) => this.terminate(reason)).next();

    if (this.status === "active") this._initCleanup = this._options?.init?.(this);
  }
  async *[Symbol.asyncIterator]() {
    const buffer = new DefaultQueue<VALUE>();
    let resolve: (() => void) | undefined;

    const consumer = this.consume((_, value) => {
      buffer.enqueue(value);
      resolve?.();
    });

    try {
      while (consumer.status === "active" || consumer.status === "drain" || buffer.size > 0) {
        if (buffer.size === 0) {
          await new Promise<void>((r) => (resolve = r));
          resolve = undefined;
        }
        while (buffer.size > 0) {
          yield buffer.dequeue() as VALUE;
        }
        consumer.next();
      }
    } catch (e) {
      consumer.terminate("abort");
    } finally {
      consumer.terminate("complete");
    }
  }
  [Symbol.dispose]() {
    this.terminate("abort");
  }
  get consumersCount(): number {
    return this._consumerSet?.size ?? 0;
  }
  get status(): Producer.Status {
    return this._status;
  }

  push(value: VALUE): this {
    this._pulling = false;
    this._consumerSet?.push(value);
    this._options?.push?.(this, value);
    return this;
  }

  private _addConsumer(consumer: Consumer<VALUE>): ConsumerSet.Delete {
    if (!this._consumerSet) this._consumerSet = this._options?.consumerSetFactory?.() ?? new DefaultConsumerSet();

    const deleteConsumer = this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options?.firstConsumerJoin?.(this, consumer);

      if (this._options?.source)
        this._sourceConsumer = this._options.source.consume(
          (_, value) => {
            this._pulling = false;
            this._consumerSet?.push(value);
            this._options?.push?.(this, value);
          },
          {
            terminate: (_, reason) => {
              this.terminate(reason);
            },
          },
        );
    }

    this._options?.consumerJoin?.(this, consumer);
    return deleteConsumer;
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    const consumer = new Consumer(handler, {
      ...options,
      queueFactory: options?.queueFactory ?? this._options?.consumerQueueFactory,
      next: (consumer) => {
        if (this._pulling === false) {
          this._pulling = true;
          this._sourceConsumer?.next();
          this._options?.next?.(this, consumer);
        }
        options?.next?.(consumer);
      },

      terminate: (consumer, reason) => {
        if (deleteConsumer()) {
          this._options?.consumerLeft?.(this, consumer);

          if (!this._consumerSet?.size) {
            this._consumerSet = undefined;

            this._options?.lastConsumerLeft?.(this, consumer);

            if (this._status === "drain") this.terminate("complete");
          }
        }

        options?.terminate?.(consumer, reason);
      },
    });

    const deleteConsumer = this._addConsumer(consumer);

    return consumer;
  }
  terminate(reason: TerminateReason): this {
    this.push = EMPTY_THIS_FUNCTION;
    this.consume = (handler, options) => new Consumer(handler, options).terminate(reason);

    if (reason === "abort") {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "abort";
    } else if (this.consumersCount) {
      this._status = "drain";
      this._options?.drain?.(this);

      this._consumerSet?.terminate("complete");
      this._consumerSet = undefined;

      return this;
    } else {
      this.terminate = EMPTY_THIS_FUNCTION;
      this._status = "complete";
    }

    this._initCleanup?.(reason);
    this._consumerSet?.terminate(reason);
    this._sourceConsumer?.terminate(reason);
    this._signalConsumer?.terminate(reason);

    this._options?.terminate?.(this, reason);

    this._options = this._consumerSet = this._sourceConsumer = this._signalConsumer = undefined;
    return this;
  }
  asSource(): Consumable<VALUE> {
    return (this._sourceProxy ??= new SourceProxy(this));
  }
}

export namespace Producer {
  export type Status = "active" | "drain" | TerminateReason;

  export type Options<VALUE> = {
    source?: Consumable<VALUE>;
    signal?: Consumable<TerminateReason>;
    consumerSetFactory?: () => ConsumerSet<VALUE>;
    consumerQueueFactory?: () => Queue<VALUE>;
    init?: (producer: Producer<VALUE>) => undefined | ((reason: TerminateReason) => void);
    push?: (producer: Producer<VALUE>, value: VALUE) => void;
    next?: (producer: Producer<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerJoin?: (producer: Producer<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (producer: Producer<VALUE>, consumer: Consumer<VALUE>) => void;
    firstConsumerJoin?: (producer: Producer<VALUE>, consumer: Consumer<VALUE>) => void;
    lastConsumerLeft?: (producer: Producer<VALUE>, consumer: Consumer<VALUE>) => void;
    drain?: (producer: Producer<VALUE>) => void;
    terminate?: (producer: Producer<VALUE>, reason: TerminateReason) => void;
  };
}
