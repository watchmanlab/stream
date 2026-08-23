import { TerminateReason } from "./types";
import { Consumer } from "./consumer";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set";
import { Source } from "./source";
import { Consumable } from "./consumable";
import { ConsumerSet } from "./consumer-set";

export class Stream<T> extends Source<T> implements Disposable, AsyncIterable<T> {
  protected consumerSet: ConsumerSet<T> = new DefaultConsumerSet();
  private status: Stream.Status = "active";
  private events?: Stream.Events<T>;
  protected push($stream: Stream<T>, value: T) {}
  protected next($stream: Stream<T>, consumer: Consumer<T>) {}
  protected consumerJoin($stream: Stream<T>, consumer: Consumer<T>) {}
  protected consumerLeft($stream: Stream<T>, consumer: Consumer<T>) {}
  protected firstConsumerJoin($stream: Stream<T>, consumer: Consumer<T>) {}
  protected lastConsumerLeft($stream: Stream<T>, consumer: Consumer<T>) {}
  protected drain($stream: Stream<T>) {}
  protected terminate($stream: Stream<T>, reason: TerminateReason) {}
  [Symbol.dispose]() {
    Stream.terminate(this, "abort");
  }

  // get $terminate(): Source<TerminateReason> {
  //   return Source.from(
  //     (this._events.$terminate ??= new Stream<TerminateReason>({
  //       lastConsumerLeft: () => (this._events.$terminate = undefined),
  //       consumerJoin: (stream, consumer) => {
  //         if (this._status === "abort" || this._status === "complete") {
  //           consumer.push(this._status);
  //           consumer.terminate(this._status);
  //           stream.terminate(this._status);
  //         }
  //       },
  //     })),
  //   );
  // }

  private static Consumer = class<T> extends Consumer<T> {
    constructor(
      private $stream: Stream<T>,
      private consumer: Consumer<T>,
    ) {
      super();
    }
    protected override handler(consumer: Consumer<T>, value: T): void {
      Consumer.push(consumer, value);
    }
    protected override next(consumer: Consumer<T>): void {
      this.$stream.next(this.$stream, this);
      if (this.$stream.events?.$next) Stream.push(this.$stream.events.$next, this);
      Consumer.next(consumer);
    }
    protected override terminate(consumer: Consumer<T>, reason: TerminateReason): void {
      if (this.$stream.consumerSet.delete(consumer)) {
        _options?.consumerLeft?.(this, consumer);
        _events.$consumerLeft?.push(consumer);

        if (!_consumerSet.size) {
          _options?.lastConsumerLeft?.(this, consumer);
          _events.$lastConsumerLeft?.push(consumer);
          if (this._status === "drain") this.terminate("complete");
        }
      }
      options?.terminate?.(consumer, reason);
    }
  };
  consume(consumer: Consumer<T>): Consumer<T> {
    const consumer = new Consumer();

    this._consumerSet.add(consumer);

    if (this._consumerSet.size === 1) {
      this._options?.firstConsumerJoin?.(this, consumer);
      this._events.$firstConsumerJoin?.push(consumer);
    }

    this._options?.consumerJoin?.(this, consumer);
    this._events.$consumerJoin?.push(consumer);

    return consumer;
  }
  static getConsumersCount<T>($stream: Stream<T>) {
    return $stream.consumerSet.size;
  }
  static push<T>($stream: Stream<T>, value: T) {
    $stream.push($stream, value);
    if ($stream.events?.$push) Stream.push($stream.events?.$push, value);
    $stream.consumerSet.push(value);
  }
  static terminate<T>(stream: Stream<T>, reason: TerminateReason) {
    stream.consume = (consumer) => Consumer.terminate(consumer, "complete");

    if (reason === "abort") {
      stream.status = "abort";
    } else if (stream.consumerSet.size) {
      stream.status = "drain";
      stream.drain(stream);
      if (stream.events?.$drain) Stream.push(stream.events.$drain, undefined);

      stream.consumerSet.terminate("complete");

      return stream;
    } else {
      stream.status = "complete";
    }

    stream.consumerSet.terminate(reason);

    stream.terminate(stream, reason);
    if (stream.events?.$terminate) Stream.push(stream.events.$terminate, reason);

    Object.values(stream.events ?? {}).forEach((stream: Stream<any>) => Stream.terminate(stream, reason));

    stream.events = undefined;

    return stream;
  }
  static override from<T>($consumable: Consumable<T>): Stream<T> {
    return new StreamFrom($consumable);
  }
}

class StreamFrom<T> extends Stream<T> {
  private pulling = false;
  private consumableConsumer!: Consumer<T>;

  constructor(private $consumable: Consumable<T>) {
    super();
  }
  protected override firstConsumerJoin($stream: Stream<T>, consumer: Consumer<T>): void {
    this.consumableConsumer = this.$consumable.consume(new StreamFrom.ConsumableConsumer(this));
  }
  protected override next($stream: Stream<T>, consumer: Consumer<T>): void {
    if (!this.pulling) {
      this.pulling = true;
      Consumer.next(this.consumableConsumer);
    }
  }
  private static ConsumableConsumer = class<T> extends Consumer<T> {
    constructor(private $streamFrom: StreamFrom<T>) {
      super();
    }
    protected override handler(consumer: Consumer<T>, value: T): void {
      this.$streamFrom.pulling = false;
      Stream.push(this.$streamFrom, value);
    }
    protected override terminate(consumer: Consumer<T>, reason: TerminateReason): void {
      Stream.terminate(this.$streamFrom, reason);
    }
  };
}

export namespace Stream {
  export type AnyStream = Stream<any>;
  export type Status = "active" | "drain" | TerminateReason;

  export interface Options<T> {}
  export type Events<T> = {
    $push?: Stream<T>;
    $next?: Stream<Consumer<T>>;
    $drain?: Stream<void>;
    $consumerJoin?: Stream<Consumer<T>>;
    $consumerLeft?: Stream<Consumer<T>>;
    $firstConsumerJoin?: Stream<Consumer<T>>;
    $lastConsumerLeft?: Stream<Consumer<T>>;
    $terminate?: Stream<TerminateReason>;
  };
}
