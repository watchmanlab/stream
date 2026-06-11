export class Vapor<VALUE> {
  private subscriptions: Vapor.Subscription<VALUE>[] = [];

  // The Shared Queue State
  private sharedQueue: { value: VALUE; pending: number }[] = [];
  private queueHead = 0; // Tracks the start of the valid data in the array

  emit(value: VALUE) {
    const subs = this.subscriptions;
    const len = subs.length;

    let pendingCount = 0;

    // Phase 1: Try the hot path first for ready subscribers
    for (let i = 0; i < len; i++) {
      const sub = subs[i]!;
      if (sub.isReady) {
        sub.executeHot(value);
      } else {
        // Consumer is blocked; they will need to pull this later
        pendingCount++;
        // Tell the subscriber exactly where their next item is waiting
        if (sub.tailPointer === -1) {
          sub.tailPointer = this.sharedQueue.length;
        }
      }
    }

    // Phase 2: Only allocate memory if someone was actually blocked
    if (pendingCount > 0) {
      this.sharedQueue.push({ value, pending: pendingCount });
    }
  }

  listen(listener: Vapor.Listener<VALUE>): Vapor.Abort {
    let isProcessing = false;

    const sub: Vapor.Subscription<VALUE> = {
      listener,
      abort,
      // Pointer into the shared queue. -1 means it's caught up to the hot path.
      tailPointer: -1,
      isReady: true,
      index: this.subscriptions.length,

      ready: () => {
        sub.isReady = true;
        sub.drain();
      },

      executeHot: (value) => {
        if (isProcessing) return; // Trampoline protection

        isProcessing = true;
        sub.isReady = false;
        try {
          listener({ value, ready: sub.ready, abort: sub.abort });
        } finally {
          isProcessing = false;
        }

        // If they called ready() synchronously during the hot path execution,
        // they are ready for whatever might be waiting in the queue
        if (sub.isReady) {
          sub.drain();
        }
      },

      drain: () => {
        if (isProcessing) return; // Trampoline guard: loop higher up handles it

        isProcessing = true;
        try {
          // Flattened execution loop pulling from the shared queue
          while (sub.isReady && sub.tailPointer !== -1 && sub.tailPointer < this.sharedQueue.length) {
            sub.isReady = false;

            const item = this.sharedQueue[sub.tailPointer]!;
            const value = item.value;

            // Decrement the reference counter
            item.pending--;

            // High-Performance Garbage Collection Compaction
            // If the item at the absolute front of the queue is fully drained,
            // drop it and slide the head forward.
            while (this.queueHead < this.sharedQueue.length && this.sharedQueue[this.queueHead]?.pending === 0) {
              (this.sharedQueue[this.queueHead] as any) = null; // Free memory immediately
              this.queueHead++;
            }

            // Advance this specific subscriber's pointer
            sub.tailPointer++;
            if (sub.tailPointer >= this.sharedQueue.length) {
              sub.tailPointer = -1; // Fully caught up to the hot stream!
            }

            // Hand control to user space
            listener({ value, ready: sub.ready, abort: sub.abort });
          }
        } finally {
          isProcessing = false;
        }
      },
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

      // Clean up reference counting if this subscription aborts while holding items
      if (sub.tailPointer !== -1) {
        for (let i = sub.tailPointer; i < self.sharedQueue.length; i++) {
          if (self.sharedQueue[i]) {
            self.sharedQueue[i]!.pending--;
          }
        }
        while (self.queueHead < self.sharedQueue.length && self.sharedQueue[self.queueHead]?.pending === 0) {
          (self.sharedQueue[self.queueHead] as any) = null;
          self.queueHead++;
        }
      }
    }
  }
}

export namespace Vapor {
  export type Abort = () => void;
  export type Ready = () => void;
  export type Listener<VALUE> = (props: { value: VALUE; ready: Ready; abort: Abort }) => void;
  export type Subscription<VALUE> = {
    listener: Listener<VALUE>;
    abort: Abort;
    ready: Ready;
    drain: () => void;
    executeHot: (value: VALUE) => void;
    isReady: boolean;
    tailPointer: number;
    index: number;
  };
}

function bench() {
  const MAX = 100_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start));

    // if (value === 1000) {
    //   queueMicrotask(() => {
    //     console.log("promise resolved", value);
    //     ready();
    //   });
    //   return;
    // }
    ready();
  });

  for (let i = 0; i <= MAX; i++) {
    vapor.emit(i);
  }
}

bench(); //foo 100 000 000 before 887. after 785

function sequential() {
  const vapor = new Vapor<number>();

  vapor.listen(async ({ value, ready }) => {
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("sequential", value);
    ready();
  });

  vapor.emit(1);
  vapor.emit(2);
  vapor.emit(3);
}

// sequential();
function consurrent() {
  const vapor = new Vapor<number>();

  vapor.listen(async ({ value, ready }) => {
    ready();
    await new Promise((r) => setTimeout(r, Math.random() * 1000));
    console.log("concurrent", value);
  });

  vapor.emit(1);
  vapor.emit(2);
  vapor.emit(3);
}

// consurrent();
