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
    let syncReadyRequested = false;

    const sub: Vapor.Subscription<VALUE> = {
      listener,
      abort,
      ready: () => {
        // Condition A: User called ready() SYNCHRONOUSLY during listener thread
        if (isProcessing) {
          syncReadyRequested = true;
          return; // Return immediately to avoid re-entering drain on a deep stack frame
        }

        // Condition B: User called ready() ASYNCHRONOUSLY later on
        sub.isReady = true;
        sub.drain();
      },
      drain: () => {
        // GATEKEEPER CHECK:
        // Only proceed if the consumer is explicitly ready AND there is data to process
        if (!sub.isReady || !sub.queue.size) {
          return;
        }

        // Consume exactly ONE value to respect backpressure
        sub.isReady = false;
        const value = sub.queue.dequeue() as VALUE;

        // Set re-entrancy processing guards
        isProcessing = true;
        syncReadyRequested = false;

        try {
          // Hand control over to user code
          listener({ value, ready: sub.ready, abort: sub.abort });
        } finally {
          isProcessing = false;
        }

        // TRAMPOLINE EVALUATION:
        // If the user synchronously called ready() during the execution block above,
        // syncReadyRequested was set to true.
        if (syncReadyRequested) {
          sub.isReady = true;
          sub.drain(); // Flat loop execution of the next single item
        }
      },
      enqueue: (value) => {
        sub.queue.enqueue(value);
        // If the user was already sitting idle waiting for data, wake up the drain pipeline
        if (sub.isReady) {
          sub.drain();
        }
      },
      queue: new Queue(),
      isReady: true, // Start true so the very first emit passes through seamlessly
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
  const MAX = 100_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start));
    ready();
  });
  //   vapor.listen(({ value, ready, abort }) => {
  //     if (value === MAX) console.log("bar", value.toLocaleString("fr"), Math.round(performance.now() - start));
  //     abort();
  //   });

  for (let i = 0; i <= MAX; i++) {
    vapor.emit(i);
  }
}

test(); //foo 100 000 000 887
