import { TerminateReason } from "./types0";
import { Consumer } from "./consumer0";
import { EMPTY_THIS_FUNCTION } from "./consts";
import { DefaultConsumerSet } from "./default-consumer-set0";
import { Source } from "./source0";
import { Consumable } from "./consumable0";
import { ConsumerSet } from "./consumer-set0";
import { Queue } from "./queue0";

export interface Stream<VALUE> extends Source<VALUE> {
  status: Stream.Status;
  readonly options: Stream.Options<VALUE>;
  readonly consumerSet: ConsumerSet<VALUE>;
  initCleanup: ((reason: TerminateReason) => void) | null;

  readonly events: {
    $push?: Stream<VALUE>;
    $next?: Stream<Consumer<VALUE>>;
    $consumerJoin?: Stream<Consumer<VALUE>>;
    $consumerLeft?: Stream<Consumer<VALUE>>;
    $firstConsumerJoin?: Stream<Consumer<VALUE>>;
    $lastConsumerLeft?: Stream<Consumer<VALUE>>;
    $drain?: Stream<void>;
    $terminate?: Stream<TerminateReason>;
  };
}

export namespace Stream {
  export type Status = "active" | "drain" | "abort" | "complete";

  export type Options<VALUE> = {
    consumerSetFactory?: () => ConsumerSet<VALUE>;
    consumerQueueFactory?: () => Queue<VALUE>;
    init?: (stream: Stream<VALUE>) => undefined | ((reason: TerminateReason) => void);
    push?: (stream: Stream<VALUE>, value: VALUE) => void;
    next?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    drain?: (stream: Stream<VALUE>) => void;
    terminate?: (stream: Stream<VALUE>, reason: TerminateReason) => void;
    consumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    consumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    firstConsumerJoin?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
    lastConsumerLeft?: (stream: Stream<VALUE>, consumer: Consumer<VALUE>) => void;
  };

  export function create<VALUE>(options?: Stream.Options<VALUE>): Stream<VALUE> {
    const stream: Stream<VALUE> = {
      [Symbol.asyncIterator]() {
        return Consumable.toAsyncIterable(this)[Symbol.asyncIterator]();
      },

      pipe(transform) {
        return Consumable.pipe(this, transform);
      },
      status: "active",
      options: { ...options },
      consumerSet: options?.consumerSetFactory?.() ?? new DefaultConsumerSet(),
      initCleanup: null,
      events: {},
      consume(handler, options) {
        return consume(stream, handler, options);
      },
    };

    stream.initCleanup = options?.init?.(stream) ?? null;

    return stream;
  }

  export function getEventSource<T, VALUE>(stream: Stream<VALUE>, key: keyof Stream<VALUE>["events"]): Stream<any> {
    if (!stream.events[key]) {
      stream.events[key] = create<any>({
        lastConsumerLeft: () => {
          stream.events[key] = undefined;
        },
      });
    }
    return stream.events[key]!;
  }

  export function push<VALUE>(stream: Stream<VALUE>, value: VALUE): void {
    if (stream.status !== "active") return;

    stream.options.push?.(stream, value);

    if (stream.events.$push) push(stream.events.$push, value);

    stream.consumerSet.push(value);
  }

  export function consume<VALUE>(
    stream: Stream<VALUE>,
    handler: Consumer.Handler<VALUE>,
    options?: Consumer.Options<VALUE>,
  ): Consumer<VALUE> {
    if (stream.status !== "active") {
      const deadConsumer = Consumer.create(handler, options);
      Consumer.terminate(deadConsumer, stream.status as TerminateReason);
      return deadConsumer;
    }

    const { queueFactory, next, terminate, ...rest } = options ?? {};

    // Reconstruct the lifecycle binding payload as a plain object contract
    const consumer = Consumer.create(handler, {
      ...rest,
      queueFactory: queueFactory ?? stream.options?.consumerQueueFactory,
      next: (c) => {
        stream.options.next?.(stream, c);
        if (stream.events.$next) push(stream.events.$next, c);
        next?.(c);
      },
      terminate: (c, reason) => {
        const deleted = deletConsumer();
        if (deleted) {
          stream.options.consumerLeft?.(stream, c);
          if (stream.events.$consumerLeft) push(stream.events.$consumerLeft, c);

          if (stream.consumerSet.size === 0) {
            stream.options.lastConsumerLeft?.(stream, c);
            if (stream.events.$lastConsumerLeft) push(stream.events.$lastConsumerLeft, c);
            if (stream.status === "drain") Stream.terminate(stream, "complete");
          }
        }
        terminate?.(c, reason);
      },
    });

    // Add consumer step tracking
    const deletConsumer = stream.consumerSet.add(consumer);

    if (stream.consumerSet.size === 1) {
      stream.options.firstConsumerJoin?.(stream, consumer);
      if (stream.events.$firstConsumerJoin) push(stream.events.$firstConsumerJoin, consumer);
    }

    stream.options.consumerJoin?.(stream, consumer);
    if (stream.events.$consumerJoin) push(stream.events.$consumerJoin, consumer);

    return consumer;
  }

  export function terminate<VALUE>(stream: Stream<VALUE>, reason: TerminateReason): void {
    if (stream.status === "abort" || stream.status === "complete") return;

    if (reason === "abort") {
      stream.status = "abort";
    } else if (stream.consumerSet.size > 0) {
      stream.status = "drain";
      stream.options.drain?.(stream);
      if (stream.events.$drain) push(stream.events.$drain, undefined);
      stream.consumerSet.terminate("complete");
      return;
    } else {
      stream.status = "complete";
    }

    stream.consumerSet.terminate(reason);
    if (stream.initCleanup) {
      stream.initCleanup(reason);
      stream.initCleanup = null;
    }

    stream.options.terminate?.(stream, reason);
    if (stream.events.$terminate) push(stream.events.$terminate, reason);

    for (const key in stream.events) {
      const $event = stream.events[key as keyof Stream<VALUE>["events"]];
      if ($event) terminate($event as any, reason);
    }
  }

  export function from<VALUE>($consumable: Consumable<VALUE>, options?: Options<VALUE>): Stream<VALUE> {
    let pulling = false;
    let consumer$: Consumer<VALUE>;

    const stream = create<VALUE>({
      ...options,
      firstConsumerJoin(s, c) {
        options?.firstConsumerJoin?.(s, c);
        consumer$ = $consumable.consume((_, value) => push(s, value), {
          terminate: (_, reason) => Stream.terminate(s, reason),
        });
      },
      push(s, value) {
        options?.push?.(s, value);
        pulling = false;
      },
      next(s, c) {
        options?.next?.(s, c);
        if (!pulling) {
          pulling = true;
          Consumer.next(consumer$);
        }
      },
    });

    return stream;
  }
}
