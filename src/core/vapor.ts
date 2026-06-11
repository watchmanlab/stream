export class Vapor<VALUE> {
  private _subscriptions: Vapor.Subscription<VALUE>[] = [];

  // The Shared Queue State
  private _sharedQueue: { value: VALUE; pending: number }[] = [];
  private _queueHead = 0; // Tracks the start of the valid data in the array
  private _subscriptionsCount = 0;
  private _state: Vapor.State = "active";
  private _errors?: Vapor<any>;

  constructor() {}

  emit(value: VALUE) {
    const subs = this._subscriptions;
    const len = subs.length;
    let pendingCount = 0;

    // Pristine Phase 1: Pure hot path loop (No isDead branch overhead)
    for (let i = 0; i < len; i++) {
      const sub = subs[i]!;
      if (sub.isReady) {
        sub.executeHot(value);
      } else {
        pendingCount++;
        if (sub.tailPointer === -1) {
          sub.tailPointer = this._sharedQueue.length;
        }
      }
    }

    if (pendingCount > 0) {
      this._sharedQueue.push({ value, pending: pendingCount });
    }
  }

  listen(listener: Vapor.Listener<VALUE>): Vapor.Abort {
    let isProcessing = false;

    const sub: Vapor.Subscription<VALUE> = {
      listener,
      abort,
      tailPointer: -1,
      isReady: true,
      index: this._subscriptions.length,

      ready: () => {
        sub.isReady = true;
        sub.drain();
      },

      executeHot: (value) => {
        if (isProcessing) return;

        isProcessing = true;
        sub.isReady = false;
        try {
          listener({ value, ready: sub.ready, abort: sub.abort });
        } finally {
          isProcessing = false;
        }

        if (sub.isReady) {
          sub.drain();
        }
      },

      drain: () => {
        if (isProcessing) return;

        isProcessing = true;
        try {
          while (sub.isReady && sub.tailPointer !== -1 && sub.tailPointer < this._sharedQueue.length) {
            sub.isReady = false;

            const item = this._sharedQueue[sub.tailPointer]!;
            const value = item.value;

            item.pending--;

            // Fast compaction
            while (this._queueHead < this._sharedQueue.length && this._sharedQueue[this._queueHead]?.pending === 0) {
              (this._sharedQueue[this._queueHead] as any) = Vapor.EMPTY;
              this._queueHead++;
            }

            // High-Performance Termination Hook Check
            // We check this at the boundary when the queue actually empties out
            if (this._queueHead === this._sharedQueue.length && this._state === "drain") {
              this._completeInstant();
            }

            sub.tailPointer++;
            if (sub.tailPointer >= this._sharedQueue.length) {
              sub.tailPointer = -1;
            }

            listener({ value, ready: sub.ready, abort: sub.abort });
          }
        } finally {
          isProcessing = false;
        }
      },
    };

    this._subscriptions.push(sub);
    this._subscriptionsCount++;
    const self = this;
    return abort;

    function abort() {
      const index = self._subscriptions.indexOf(sub);
      if (index >= 0) {
        self._subscriptionsCount--;
        const last = self._subscriptions.pop()!;
        if (index < self._subscriptions.length) {
          (self._subscriptions[index] = last).index = index;
        }
      }

      if (sub.tailPointer !== -1) {
        for (let i = sub.tailPointer; i < self._sharedQueue.length; i++) {
          if (self._sharedQueue[i]) {
            self._sharedQueue[i]!.pending--;
          }
        }
        while (self._queueHead < self._sharedQueue.length && self._sharedQueue[self._queueHead]?.pending === 0) {
          (self._sharedQueue[self._queueHead] as any) = Vapor.EMPTY;
          self._queueHead++;
        }
      }
    }
  }

  terminate<ERROR>(reason: Vapor.TerminateReason<ERROR>) {
    switch (reason.type) {
      case "abort":
        this.emit =
          (this.listen as any) =
          this.terminate =
            () => this._throw(new Vapor.EmitException(`terminated`, { cause: "aborted" }));
        this._state = "aborted";
        this._clear();
        break;
      case "error":
        this.emit =
          (this.listen as any) =
          this.terminate =
            () => this._throw(new Vapor.EmitException(`terminated`, { cause: reason.error }));
        this._state = "error";
        this._throw(reason.error);
        this._clear();
        break;
      case "complete":
        if (this._state === "active") {
          // If the queue is already empty, terminate instantly
          if (this._queueHead >= this._sharedQueue.length) {
            this._completeInstant();
          } else {
            // Otherwise, block future emits and wait for drain loop to finish
            this.emit = () => this._throw(new Vapor.EmitException(`terminated`, { cause: "drain" }));
            this._state = "drain";
          }
        }
        break;
    }
  }

  private _completeInstant() {
    this.emit =
      (this.listen as any) =
      this.terminate =
        () => this._throw(new Vapor.EmitException(`terminated`, { cause: "completed" }));
    this._state = "completed";
  }

  private _clear() {
    this._queueHead = 0;
    this._sharedQueue.length = 0;
    this._subscriptions.length = 0;
    this._subscriptionsCount = 0;
    this._errors?.terminate({ type: "complete" });
    this._errors = undefined;
  }

  private _throw<ERROR>(error: ERROR) {
    if (!this._errors || !this._errors.subscriptionsCount) {
      Promise.reject(error);
    } else {
      this._errors.emit(error);
    }
  }

  get subscriptionsCount() {
    return this._subscriptionsCount;
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

  export type State = "active" | "drain" | "completed" | "aborted" | "error";
  export type TerminateReason<ERROR> = { type: "complete" } | { type: "abort" } | { type: "error"; error: ERROR };

  export class EmitException extends Error {
    constructor(message?: string, options?: ErrorOptions) {
      super(message, options);
    }
  }

  export const EMPTY = Symbol.for("empty");
  export type Empty = typeof EMPTY;
}

function bench() {
  const MAX = 100_000_000;
  const vapor = new Vapor<number>();

  const start = performance.now();

  vapor.listen(({ value, ready }) => {
    if (value === MAX) console.log("foo", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");

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

bench(); //foo 100 000 000  785 ms

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
