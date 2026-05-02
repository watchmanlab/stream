import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends string> implements AsyncIterableIterator<VALUE>, Disposable {
  private _queue: Queue<VALUE, `${NAME}Queue`>;
  private _pendings: Queue<(value: VALUE | Queue.Empty) => void, `${NAME}Pending`>;
  private _valueQueued?: Stream<VALUE, never, `${NAME}ValueQueued`>;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _valueDropped?: Stream<VALUE, never, `${NAME}ValueDropped`>;
  private _terminated?: Stream<undefined, never, `${NAME}Terminated`>;
  private _isTerminated = false;
  constructor(
    public readonly name: NAME,
    private options: Consumer.Options<VALUE> = {},
  ) {
    this._queue = new Queue(`${this.name}Queue`, options.queueOptions);
    this._pendings = new Queue(`${this.name}Pending`, options.pendingsOptions);
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  [Symbol.dispose]() {
    this.return();
  }

  push<T extends VALUE>(value: T) {
    const pending = this._pendings.dequeue();
    if (pending !== Queue.EMPTY) {
      pending(value);
    } else {
      this._queue.enqueue(value);
      this._valueQueued?.push(value);
    }

    return new Consumer.PushProgress(this.name, value, this);
  }
  private _currentValue: VALUE | Queue.Empty = Queue.EMPTY;
  async next(): Promise<IteratorResult<VALUE, any>> {
    if (this._currentValue !== Queue.EMPTY) {
      this._valueProcessed?.push(this._currentValue);
      this._currentValue = Queue.EMPTY;
    }
    if (this._isTerminated) return { value: Queue.EMPTY as never, done: true };

    const value = this._queue.dequeue();
    if (value !== Queue.EMPTY) {
      this._valueProcessing?.push(value);
      this._currentValue = value;
      return { value };
    } else {
      const value = await new Promise<VALUE | Queue.Empty>((r) => {
        this._pendings.enqueue(r);
        if (this.options?.source?.idle) this.options.source.requestNext();
      });
      return { value: value as never, done: value === Queue.EMPTY };
    }
  }
  async return(): Promise<IteratorResult<VALUE, any>> {
    this._isTerminated = true;
    for (const waiter of this._pendings) {
      waiter(Queue.EMPTY);
    }
    this._pendings.clear();

    for (const value of this._queue) {
      this._valueDropped?.push(value);
    }
    this._queue.clear();

    this._valueQueued?.terminate();
    this._valueProcessing?.terminate();
    this._valueProcessed?.terminate();
    this._valueDropped?.terminate();
    this._valueQueued = undefined;
    this._valueProcessing = undefined;
    this._valueProcessed = undefined;
    this._valueDropped = undefined;

    this._terminated?.push(undefined);
    this._terminated?.terminate();
    this._terminated = undefined;
    this.options?.onTerminate?.();
    return { value: Queue.EMPTY as never, done: true };
  }
  get queue() {
    return this._queue;
  }
  get pending() {
    return this._pendings;
  }
  get valueQueued() {
    if (!this._valueQueued) this._valueQueued = new Stream(`${this.name}ValueQueued`);
    return this._valueQueued;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get valueDropped() {
    if (!this._valueDropped) this._valueDropped = new Stream(`${this.name}ValueDropped`);
    return this._valueDropped;
  }
}
export namespace Consumer {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
    onTerminate?: () => void;
    queueOptions?: Queue.Options;
    pendingsOptions?: Queue.Options;
  };

  export class PushProgress<const VALUE, NAME extends string> {
    private _queued?: Stream<Awaited<VALUE>, never, `${NAME}Queued`>;
    private _processing?: Stream<Awaited<VALUE>, never, `${NAME}Processing`>;
    private _processed?: Stream<Awaited<VALUE>, never, `${NAME}Processed`>;
    private _dropped?: Stream<Awaited<VALUE>, never, `${NAME}Dropped`>;
    constructor(
      public readonly name: NAME,
      private value: VALUE,
      private consumer: Consumer<VALUE, NAME>,
    ) {}

    get queued() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._queued)
        this._queued = new Stream(`${this.name}Queued`, async function* () {
          for await (const value of consumer.valueQueued) {
            if (value === searchValue) {
              yield searchValue;
              break;
            }
          }
        });
      return this._queued;
    }
    get processing() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._processing)
        this._processing = new Stream(`${this.name}Processing`, async function* () {
          for await (const value of consumer.valueProcessing) {
            if (value === searchValue) {
              yield searchValue;
              break;
            }
          }
        });
      return this._processing;
    }
    get processed() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._processed)
        this._processed = new Stream(`${this.name}Processed`, async function* () {
          for await (const value of consumer.valueProcessed) {
            if (value === searchValue) {
              yield searchValue;
              break;
            }
          }
        });
      return this._processed;
    }
    get dropped() {
      const [consumer, searchValue] = [this.consumer, this.value];
      if (!this._dropped)
        this._dropped = new Stream(`${this.name}Dropped`, async function* () {
          for await (const value of consumer.valueDropped) {
            if (value === searchValue) {
              yield searchValue;
              break;
            }
          }
        });
      return this._dropped;
    }
  }
}
