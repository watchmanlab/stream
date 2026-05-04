import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

const NAME = "consumer";
export class Consumer<
  VALUE,
  NAME extends string = Consumer.Name,
  BUFFER extends Queue<VALUE, any> = Queue<VALUE, `${NAME}Queue`>,
  PENDINGS extends Queue<(value: VALUE | Queue.Empty) => void, any> = Queue<
    (value: VALUE | Queue.Empty) => void,
    `${NAME}Pending`
  >,
  SOURCE extends Source<VALUE, any, any> = Source<VALUE, any, `${NAME}Source`>,
>
  implements AsyncIterableIterator<VALUE>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _options: Consumer.Options<VALUE, BUFFER, PENDINGS, SOURCE>;
  private _buffer: BUFFER;
  private _pendings: PENDINGS;
  private _valueProcessing?: Stream<VALUE, never, `${NAME}ValueProcessing`>;
  private _valueProcessed?: Stream<VALUE, never, `${NAME}ValueProcessed`>;
  private _disposed?: Stream<undefined, never, `${NAME}Disposed`>;

  constructor(name: NAME, options?: Consumer.Options<VALUE, BUFFER, PENDINGS, SOURCE>);
  constructor(options?: Consumer.Options<VALUE, BUFFER, PENDINGS, SOURCE>);
  constructor(
    nameOrOptions?: NAME | Consumer.Options<VALUE, BUFFER, PENDINGS, SOURCE>,
    options?: Consumer.Options<VALUE, BUFFER, PENDINGS, SOURCE>,
  ) {
    if (typeof nameOrOptions === "string") {
      this.name = nameOrOptions;
      this._options = { ...options };
    } else {
      this.name = NAME as NAME;
      this._options = { ...nameOrOptions };
    }

    this._buffer = this._options.buffer ?? (new Queue(`${this.name}Queue`) as BUFFER);
    this._pendings = this._options.pendings ?? (new Queue(`${this.name}Pending`) as PENDINGS);
  }

  [Symbol.asyncIterator]() {
    return this;
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  push<T extends VALUE>(value: T) {
    const pending = this._pendings.dequeue();
    if (pending !== Queue.EMPTY) {
      pending(value);
    } else {
      this._buffer.enqueue(value);
    }

    return new Consumer.PushProgress(this.name, value, this);
  }
  private _currentValue: VALUE | Queue.Empty = Queue.EMPTY;
  async next(): Promise<IteratorResult<VALUE, any>> {
    if (this._currentValue !== Queue.EMPTY) {
      this._valueProcessed?.push(this._currentValue);
      this._currentValue = Queue.EMPTY;
    }

    const value = this._buffer.dequeue();
    if (value !== Queue.EMPTY) {
      this._valueProcessing?.push(value);
      this._currentValue = value;
      return { value };
    } else {
      const value = await new Promise<VALUE | Queue.Empty>((r) => {
        this._pendings.enqueue(r);
        if (this._options?.source?.idle) this._options.source.requestNext();
      });
      return { value: value as never, done: value === Queue.EMPTY };
    }
  }
  async return(): Promise<IteratorResult<VALUE, any>> {
    for (const pending of this._pendings) {
      pending(Queue.EMPTY);
    }

    await Promise.all([
      this._pendings.dispose(),
      this._buffer.dispose(),
      this._valueProcessing?.dispose(),
      this._valueProcessed?.dispose(),
    ]);

    this._valueProcessing = this._valueProcessed = undefined;

    this._disposed?.push(undefined);
    await this._disposed?.dispose();
    this._disposed = undefined;

    this._options?.onTerminate?.();
    return { value: Queue.EMPTY as never, done: true };
  }
  async dispose() {
    await this.return();
  }
  get buffer() {
    return this._buffer;
  }
  get pendings() {
    return this._pendings;
  }
  get source() {
    return this._options.source;
  }
  get valueProcessing() {
    if (!this._valueProcessing) this._valueProcessing = new Stream(`${this.name}ValueProcessing`);
    return this._valueProcessing;
  }
  get valueProcessed() {
    if (!this._valueProcessed) this._valueProcessed = new Stream(`${this.name}ValueProcessed`);
    return this._valueProcessed;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`${this.name}Disposed`);
    return this._disposed;
  }
}
export namespace Consumer {
  export type Name = typeof NAME;
  export type Options<
    VALUE,
    BUFFER extends Queue<VALUE, any>,
    PENDINGS extends Queue<(value: VALUE | Queue.Empty) => void, any>,
    SOURCE extends Source<VALUE, any, any>,
  > = {
    source?: SOURCE;
    onTerminate?: () => void;
    buffer?: BUFFER;
    pendings?: PENDINGS;
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
          for await (const value of consumer.buffer.valueQueued) {
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
          for await (const value of consumer.buffer.valueDropped) {
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
