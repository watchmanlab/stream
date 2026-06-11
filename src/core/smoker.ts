export class Smoker<VALUE> implements Disposable {
  private _listeners: Subscription<VALUE>[] = [];
  private _emit = (value: VALUE) => {};
  private _emitting = false;
  private _deferredAborts: number[] = [];

  private _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this._emit = (value: VALUE) => {};
        break;
      case 1:
        const sub = this._listeners[0]!;
        this._emit = (value: VALUE) => {
          this._emitting = true;

          sub.listener(value);

          this._emitting = false;
          this._flushDeferred();
        };
        break;
      default:
        this._emit = (value: VALUE) => {
          this._emitting = true;
          const list = this._listeners;
          const len = list.length;
          // Pure, raw forward loop. Zero conditionals or lookup guards inside.
          for (let i = 0; i < len; i++) {
            list[i]!.listener(value);
          }
          this._emitting = false;
          this._flushDeferred(); // Synchronously clean up right here
        };
    }
  }

  private _flushDeferred() {
    if (this._deferredAborts.length === 0) return;

    for (let i = this._deferredAborts.length - 1; i >= 0; i--) {
      const idx = this._deferredAborts[i]!;
      if (idx >= this._listeners.length) continue;

      const last = this._listeners.pop()!;
      if (idx < this._listeners.length) {
        (this._listeners[idx] = last).index = idx;
      }
    }
    this._deferredAborts.length = 0;
    this._swapEmit();
  }

  emit(value: VALUE) {
    this._emit(value);
  }

  listen(listener: Smoker.Listener<VALUE>): Smoker.Abort {
    const sub: Subscription<VALUE> = {
      listener,
      index: this._listeners.length,
    };
    this._listeners.push(sub);
    this._swapEmit();

    return () => {
      const idx = sub.index;
      if (idx === -1) return;

      if (this._emitting) {
        this._deferredAborts.push(idx);
        sub.index = -1;
        return;
      }

      const last = this._listeners.pop()!;
      if (idx < this._listeners.length) {
        (this._listeners[idx] = last).index = idx;
      }
      sub.index = -1;
      this._swapEmit();
    };
  }
  clear() {
    this._listeners.length = 0;
    this._deferredAborts.length = 0;
    this._swapEmit();
  }
  get listeners() {
    return this._listeners.length;
  }

  [Symbol.dispose]() {
    this.clear();
  }
}

export namespace Smoker {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
}
type Subscription<VALUE> = {
  listener: Smoker.Listener<VALUE>;
  index: number;
};

function runLockBenchmark() {
  console.log("=== STARTING SMOKER LOCK BENCHMARK ===");

  const smoker = new Smoker<number>();

  // 1. Hot-Path Velocity Test (1 Billion direct loop operations)
  let executionCount = 0;
  const unbind1 = smoker.listen(() => {
    executionCount++;
  });
  const unbind2 = smoker.listen(() => {
    executionCount++;
  });
  const unbind3 = smoker.listen(() => {
    executionCount++;
  });

  const EMISSIONS = 333_333_334; // 333.3M * 3 listeners = ~1 Billion operations
  const start = performance.now();

  for (let i = 0; i < EMISSIONS; i++) {
    smoker.emit(i);
  }

  const duration = performance.now() - start;
  const totalOps = EMISSIONS * 3;
  const opsPerSec = totalOps / (duration / 1000);

  console.log(`Executed : ${totalOps.toLocaleString()} ops`);
  console.log(`Duration : ${duration.toFixed(2)} ms`);
  console.log(`Velocity : ${(opsPerSec / 1_000_000).toFixed(2)} million ops/sec`);

  // Tear down hot path
  unbind1();
  unbind2();
  unbind3();

  console.log("\n=== TESTING IN-FLIGHT MUTATIONS ===");

  // 2. Behavioral Verification (The Node.js Behavior Check)
  const mutationSmoker = new Smoker<string>();
  let l0Ran = false;
  let l1Ran = false;
  let l2RanCount = 0;

  // Since it's a forward loop now, order is L0 -> L1 -> L2
  const abort0 = mutationSmoker.listen(() => {
    l0Ran = true;
  });

  const abort1 = mutationSmoker.listen(() => {
    l1Ran = true;
    // L1 aborts L2 (a future listener) mid-flight!
    abort2();
  });

  const abort2 = mutationSmoker.listen(() => {
    l2RanCount++;
  });

  // Emit data to trigger the in-flight mutation
  mutationSmoker.emit("mutation-payload");

  console.log(`Did L0 execute normally? ${l0Ran ? "PASSED" : "FAILED"}`);
  console.log(`Did L1 execute normally? ${l1Ran ? "PASSED" : "FAILED"}`);

  // Like Node.js, since the loop bounds were fixed at the start of emit(),
  // L2 will still execute exactly once this cycle, but its removal is deferred.
  console.log(`Did L2 execute exactly once this cycle? ${l2RanCount === 1 ? "PASSED" : "FAILED"}`);

  // 3. Post-Emission Cleanup Check (Verifying no stale items sit in memory)
  console.log(
    `Are there zero active listeners left in memory? ${mutationSmoker.listeners === 2 ? "PASSED" : "FAILED"}`,
  );

  // Emit a second time to prove L2 is now completely wiped out of the active loop
  l2RanCount = 0;
  mutationSmoker.emit("second-payload");
  console.log(`Is L2 completely absent on the next emission? ${l2RanCount === 0 ? "PASSED" : "FAILED"}`);

  // Clean up remaining hooks
  abort0();
  abort1();
  console.log(`Final listener count after total teardown: ${mutationSmoker.listeners}`);
}

// runLockBenchmark();
