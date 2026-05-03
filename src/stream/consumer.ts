import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends string> implements AsyncIterableIterator<VALUE>, AsyncDisposable, Disposable {
  private _queue: Queue<VALUE, `${NAME}Queue`>;
  private _pendings: Queue<(value: VALUE | Queue.Empty) => void, `${NAME}Pending`>;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
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
  async [Symbol.asyncDispose]() {
    await this.return();
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

    for (const pending of this._pendings) {
      pending(Queue.EMPTY);
    }

    await Promise.all([this._queue.clear(), this._valueProcessing?.terminate(), this._valueProcessed?.terminate()]);

    this._valueProcessing = undefined;
    this._valueProcessed = undefined;

    this._terminated?.push(undefined);
    await this._terminated?.terminate();
    this._terminated = undefined;

    this.options?.onTerminate?.();
    return { value: Queue.EMPTY as never, done: true };
  }

  get queue() {
    return this._queue;
  }
  get pendings() {
    return this._pendings;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get terminated() {
    if (!this._terminated) this._terminated = new Stream(`${this.name}Terminated`);
    return this._terminated;
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
          for await (const value of consumer.queue.valueQueued) {
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
          for await (const value of consumer.queue.valueDropped) {
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
