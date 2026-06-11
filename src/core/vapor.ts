import { Queue } from "../queue";
import { Smoker } from "./smoker";

export class Vapor<VALUE> {
  private subscriptions: Vapor.Subscription<VALUE>[] = [];
  //   private smoker = new Smoker<Vapor.Subscription<VALUE>>();

  constructor(source?: Vapor<VALUE>) {
    source?.listen(({ value }) => this.emit(value));

    // this.smoker.listen(sub=>{
    //     while(sub.queue.size){
    //         const value = sub.queue.dequeue()
    //     }
    // })
  }

  emit(value: VALUE) {
    const subscriptions = this.subscriptions;
    const length = subscriptions.length;
    for (let i = 0; i < length; i++) {
      const sub = subscriptions[i]!;

      if (sub.isReady) {
        sub.isReady = false;
        sub.listener({ value, abort: sub.abort, ready: sub.ready });
      } else {
        sub.enqueue(value);
      }
    }
  }

  listen(listener: Vapor.Listener<VALUE>): Vapor.Abort {
    let isProcessing = false;

    const sub: Vapor.Subscription<VALUE> = {
      listener,
      abort,
      ready: () => {
        // Safe to call anytime (sync or async)
        sub.isReady = true;
        sub.drain();
      },
      drain: () => {
        // 1. TRAMPOLINE GUARD: If drain() is already running higher up the stack,
        // stop immediately. The active loop will pull the remaining items.
        if (isProcessing) {
          return;
        }

        // 2. TRUE LOOP UNROLLING: Pull items sequentially on a flat stack frame
        isProcessing = true;
        try {
          while (sub.isReady && sub.queue.size) {
            sub.isReady = false; // Reset readiness flag for this item
            const value = sub.queue.dequeue() as VALUE;

            // Hand control over to the user code cleanly
            listener({ value, ready: sub.ready, abort: sub.abort });
          }
        } finally {
          isProcessing = false;
        }
      },
      enqueue: (value) => {
        sub.queue.enqueue(value);
        if (sub.isReady) {
          sub.drain();
        }
      },
      queue: new Queue(),
      isReady: true,
      index: this.subscriptions.length,
    };

    this.subscriptions.push(sub);
    const self = this;
    return abort;

    function abort() {
      const index = self.subscriptions.indexOf(sub);
      if (index >= 0) {
        const last = self.subscriptions.pop()!;
        if (index < self.subscriptions.length) {
          (self.subscriptions[index] = last).index = index;
        }
      }
      sub.queue.clear();
    }
  }
}
export namespace Vapor {
  export type Abort = () => void;
  export type Ready = () => void;
  export type Drain = () => void;
  export type Enqueue<VALUE> = (value: VALUE) => void;
  export type Listener<VALUE> = (props: { value: VALUE; ready: Ready; abort: Abort }) => void;
  export type Subscription<VALUE> = {
    listener: Listener<VALUE>;
    abort: Abort;
    ready: Ready;
    drain: Drain;
    enqueue: Enqueue<VALUE>;
    queue: Queue<VALUE>;
    isReady: boolean;
    index: number;
  };
}

function test() {
  const MAX = 30_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start));

    if (value === 1000) {
      queueMicrotask(() => {
        console.log("promise resolved", value);
        ready();
      });
      return;
    }
    ready();
  });

  for (let i = 0; i <= MAX; i++) {
    vapor.emit(i);
  }
}

test(); //foo 100 000 000 887
