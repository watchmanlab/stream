import { Queue } from "../queue.ts";
import { Smoker } from "./smoker.ts";

export class Channel<VALUE> implements Disposable {
  private _queue: Queue<VALUE>;
  private _pending: number;
  private _options: Channel.Options<VALUE>;

  constructor(options: Channel.Options<VALUE>) {
    this._options = { ...options };
    this._queue = this._options?.queue ? this._options.queue : new Queue();
    this._pending = 0;

    const abort = this._options.source.listen((value) => {
      if (this._pending > 0) {
        this._pending--;
        this._options.next(value, this);
      } else {
        this._queue.enqueue(value);
      }
    });

    const dispose = this._options.dispose;
    this._options.dispose = () => {
      dispose?.(this);
      abort();
    };
  }

  next() {
    let value = this._queue.dequeue();

    if (value !== Queue.EMPTY) {
      this._options.next(value, this);
      return;
    }

    this._pending++;
    this._options.pull?.(this);
  }
  dispose(): void {
    this._queue.clear();
    this._pending = 0;
    this._options.dispose?.(this);
  }
  [Symbol.dispose](): void {
    this.dispose();
  }
  get queue(): Queue<VALUE> {
    return this._queue;
  }
  get pending(): number {
    return this._pending;
  }
}

export namespace Channel {
  export type Options<VALUE> = {
    source: Smoker<VALUE>;
    next: (value: VALUE, self: Channel<VALUE>) => void;
    pull?: (self: Channel<VALUE>) => void;
    dispose?: (self: Channel<VALUE>) => void;
    queue?: Queue<VALUE>;
  };
}

function test() {
  const MAX = 100_000_000;
  const start = performance.now();
  const source = new Smoker<number>();
  const channel = new Channel({
    source: source,
    next(value) {
      if (value === MAX) {
        console.log(value.toLocaleString("fr"), "ops", Math.round(performance.now() - start), "ms");
        return;
      }
    },
  });

  for (let i = 0; i <= MAX; i++) {
    channel.next();
    source.emit(i);
  }
}

// test(); //100 000 000 ops 967 ms

function realisticTest() {
  const MAX = 50_000_000; // Lowered because queueing allocates memory
  const source = new Smoker<number>();
  const channel = new Channel({
    source: source,
    next(value) {
      /* handle */
    },
  });

  // 1. Fill the queue completely first (Tests queue write performance)
  for (let i = 0; i <= MAX; i++) {
    source.emit(i);
  }

  // 2. Consume everything (Tests queue read performance)
  const start = performance.now();
  for (let i = 0; i <= MAX; i++) {
    channel.next();
  }
  console.log(`Queue processing took: ${performance.now() - start} ms`);
}

// realisticTest();

async function asyncTest() {
  const MAX = 1_000_000; // Lowered because promises are heavy
  const source = new Smoker<number>();

  // A wrapper to turn your next() call into a Promise
  let resolveNext: (val: number) => void;
  const channel = new Channel({
    source,
    next(value) {
      resolveNext(value);
    },
  });

  const start = performance.now();

  // Simulate an async consumer reading data
  const consumer = async () => {
    for (let i = 0; i < MAX; i++) {
      const promise = new Promise<number>((r) => {
        resolveNext = r;
      });
      channel.next();
      await promise;
    }
  };

  // Simulate a producer emitting data asynchronously
  const producer = async () => {
    for (let i = 0; i < MAX; i++) {
      source.emit(i);
      await new Promise((r) => queueMicrotask(r)); // Yield control
    }
  };

  await Promise.all([consumer(), producer()]);
  console.log(`Async processing took: ${performance.now() - start} ms`);
}

// asyncTest();
function stressTestPendingBloat() {
  const source = new Smoker<number>();
  const channel = new Channel({
    source,
    next(value) {
      console.log("Received:", value);
    },
  });

  // Simulate a runaway consumer loop calling next() 1,000,000 times while empty
  for (let i = 0; i < 1_000_000; i++) {
    channel.next();
  }

  console.log("Pending count after runaway consumer:", channel.pending); // 1,000,000

  // Now emit a single valid item
  source.emit(42);
  // CRITICAL CHECK: Did next() actually handle 42, or did it just decrement _pending to 999,999 without executing your logic?
}
// stressTestPendingBloat();
