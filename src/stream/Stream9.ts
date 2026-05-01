import { Queue } from "./queue";
import { Source } from "./source";

export class Stream<VALUE, NAME extends string> {
  private _queue = new Queue<VALUE>();
  private _isTerminated = false;
  private _resolve?: (value: VALUE | Stream.Terminated) => void;
  constructor(public readonly name = "stream" as NAME) {}

  push<T extends VALUE>(value: T) {
    if (this._resolve) {
      this._resolve(value);
    } else {
      this._queue.enqueue(value);
    }
  }

  private promise?: Promise<VALUE | Stream.Terminated>;
  pull() {
    if (this._isTerminated) return Stream.TERMINATED;

    if (this.promise) return this.promise;

    const value = this._queue.dequeue();

    if (value !== Queue.EMPTY) return value;

    this.promise = new Promise<VALUE | Stream.Terminated>((r) => (this._resolve = r));
    return this.promise;
  }

  terminate() {
    this._isTerminated = true;
    this._resolve?.(Stream.TERMINATED);
    this._resolve = undefined;
    this._queue.clear();
  }
}
export namespace Stream {
  export const TERMINATED = Symbol.for("$TERMINATED#");
  export type Terminated = typeof TERMINATED;
}

function simpleTest() {
  const stream = new Stream();

  (async () => {
    while (true) {
      console.log("c1", await stream.pull());
    }
  })();
  (async () => {
    while (true) {
      console.log("c2", await stream.pull());
    }
  })();

  stream.push(44);
  stream.push(55);
}
function newStreamBench() {
  const MAX = 1_000_000;
  const now = performance.now();

  const stream = new Stream<number, never>();

  (async () => {
    while (true) {
      const value = await stream.pull();
      if (value === Stream.TERMINATED) return;
      let result = value + 10;
      if (result === 40010) {
        result = 444;
      } else {
        result = 555;
      }
      if (value === MAX) {
        console.log("new stream", Math.round(performance.now() - now));
        return;
      }
    }
  })();

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

simpleTest();
