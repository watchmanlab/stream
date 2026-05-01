import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends string> implements AsyncIterable<VALUE>, Disposable {
  private _valueQueued?: Stream<VALUE, never, `${NAME}ValueQueued`>;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _valueDropped?: Stream<VALUE, never, `${NAME}ValueDropped`>;
  private _terminated?: Stream<undefined, never, `${NAME}Terminated`>;
  private _isTerminated = false;

  private _resolve?: (value: VALUE | Consumer.Terminated) => void;
  constructor(
    public readonly name: NAME,
    public readonly queue: Queue<VALUE>,
    private options: Consumer.Options<VALUE> = {},
  ) {}

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const result = this.next();
        const value = result instanceof Promise ? await result : result;

        return { value: value as VALUE, done: value === Consumer.TERMINATED };
      },
      return: async () => {
        this.terminate();
        return { value: Consumer.TERMINATED as VALUE, done: true };
      },
    };
  }
  [Symbol.dispose]() {
    this.terminate();
  }
  push<T extends VALUE>(value: T) {
    if (this._resolve) {
      this._resolve(value);
      this._resolve = undefined;
    } else {
      this.queue.enqueue(value);
      this._valueQueued?.push(value);
    }

    return new Consumer.PushProgress(this.name, value, this);
  }
  private _currentValue: VALUE | Queue.Empty = Queue.EMPTY;
  next() {
    if (this._currentValue !== Queue.EMPTY) {
      this._valueProcessed?.push(this._currentValue);
      this._currentValue = Queue.EMPTY;
    }
    if (this._isTerminated) return Consumer.TERMINATED;

    const value = this.queue.dequeue();
    if (value !== Queue.EMPTY) {
      this._valueProcessing?.push(value);
      this._currentValue = value;
      return value;
    } else {
      this._resolve?.(Consumer.TERMINATED);
      return new Promise<VALUE | Consumer.Terminated>((r) => {
        this._resolve = r;
        if (this.options?.source?.idle) this.options.source.requestNext();
      });
    }
  }

  terminate() {
    this._isTerminated = true;
    this._resolve?.(Consumer.TERMINATED);
    this._resolve = undefined;
    for (const value of this.queue) {
      this._valueDropped?.push(value);
    }
    this.queue.clear();

    this.options?.onTerminate?.();
    this._terminated?.push(undefined);

    this._valueQueued?.terminate();
    this._valueProcessing?.terminate();
    this._valueProcessed?.terminate();
    this._valueDropped?.terminate();
    this._terminated?.terminate();
    this._valueQueued = undefined;
    this._valueProcessing = undefined;
    this._valueProcessed = undefined;
    this._valueDropped = undefined;
    this._terminated = undefined;
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

  export const TERMINATED = Symbol.for("$TERMINATED#");
  export type Terminated = typeof TERMINATED;
}
