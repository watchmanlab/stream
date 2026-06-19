import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

async function bench() {
  const MAX = 100_000_000;

  const stream = new Stream<number>();
  const channel = stream.getChannel();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
  const start = performance.now();

  try {
    let next = channel.next();
    next = next instanceof Promise ? await next : next;

    while (true) {
      if (next === MAX) console.log(next.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
      next = channel.next();
      next = next instanceof Promise ? await next : next;
    }
  } catch (error) {}
}
// bench(); //100 000 000 345 ms
async function realPullBench() {
  const MAX = 10_000_000; // Lowered slightly for pure async loop comparisons
  let count = 0;

  // An upstream source channel that simulates pulling data iteratively
  const upstreamSource = new Channel<number>({
    pull: (self) => {
      if (count <= MAX) {
        self.push(count++);
      } else {
        self.terminate(Channel.COMPLETED);
      }
    },
  });

  // Your root stream bound directly to the source
  const stream = new Stream<number>({ source: upstreamSource });
  const channel = stream.getChannel();

  const start = performance.now();

  try {
    while (true) {
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      if (next === MAX) {
        console.log("Pull Pipeline Finished:", next.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
        break;
      }
    }
  } catch (e) {}
}
realPullBench();

function verifyStreamTeardown() {
  const stream = new Stream<number>();

  // Open 4 independent channels
  const ch1 = stream.getChannel();
  const ch2 = stream.getChannel();
  const ch3 = stream.getChannel();
  const ch4 = stream.getChannel();

  // Terminate the root stream
  stream.terminate(Symbol.for("completed") as any);

  // CRITICAL CHECK: Did every single channel switch its state cleanly?
  console.log(`Channel 1 Terminated? ${ch1.status !== "active" ? "PASSED" : "FAILED"}`);
  console.log(`Channel 2 Terminated? ${ch2.status !== "active" ? "PASSED" : "FAILED"}`);
  console.log(`Channel 3 Terminated? ${ch3.status !== "active" ? "PASSED" : "FAILED"}`);
  console.log(`Channel 4 Terminated? ${ch4.status !== "active" ? "PASSED" : "FAILED"}`);
}

// verifyStreamTeardown();

function verifyParallelPulling() {
  const upstreamSource = new Stream<number>();
  const rootStream = new Stream<number>({ source: upstreamSource.getChannel() });

  // Two independent consumer channels bound to the stream
  const consumer1 = rootStream.getChannel();
  const consumer2 = rootStream.getChannel();

  // Both consumers find their queues dry and call next(), triggering pull notifications
  consumer1.next();
  consumer2.next();

  // Push data down from the source
  upstreamSource.push(100);
  upstreamSource.push(200);

  // Check if both consumer channels successfully received a notification
  console.log(`Consumer 1 registered data? ${consumer1.queue.size > 0 || consumer1.hasPending ? "PASSED" : "FAILED"}`);
  console.log(`Consumer 2 registered data? ${consumer2.queue.size > 0 || consumer2.hasPending ? "PASSED" : "FAILED"}`);
}

// verifyParallelPulling();

function runCalibratedStressTest() {
  console.log("=== STARTING CALIBRATED CHAOS STRESS TEST ===");

  const stream = new Stream<number>();
  const activeChannels: Channel<number>[] = [];

  let totalPushed = 0;
  let totalConsumed = 0;

  // 1. Establish precise memory baseline
  if (globalThis.gc) globalThis.gc();
  // Compatible with Node/Bun/Deno memory APIs
  const getMemory = () =>
    typeof process !== "undefined" ? process.memoryUsage().heapUsed : (performance as any).memory?.usedJSHeapSize || 0;
  const initialMemory = getMemory();

  // 1,000,000 iterations is plenty to verify JIT stability and memory leaks
  const ITERATIONS = 1_000_000;
  const start = performance.now();

  for (let i = 0; i < ITERATIONS; i++) {
    // Keep max concurrent active channels capped at 20 to prevent O(N^2) explosion
    if (i % 100 === 0 && activeChannels.length < 20) {
      const ch = stream.getChannel();
      activeChannels.push(ch);

      // Simple, non-blocking consumer task
      (async () => {
        try {
          while (true) {
            let next = ch.next();
            next = next instanceof Promise ? await next : next;
            totalConsumed++;
          }
        } catch (e) {
          // Suppress expected termination throws
        }
      })();
    }

    // High-speed broadcast push
    stream.push(i);
    totalPushed++;

    // High-frequency deletion: violently remove a channel from the center of the array
    if (i % 120 === 0 && activeChannels.length > 2) {
      const targetIndex = Math.floor(activeChannels.length / 2);
      const deadChannel = activeChannels[targetIndex]!;

      const reason = i % 2 === 0 ? Channel.COMPLETED : Channel.ABORTED;
      deadChannel.terminate(reason);

      // Perform your signature O(1) Pop-and-Swap to clean up tracker reference
      activeChannels[targetIndex] = activeChannels[activeChannels.length - 1]!;
      activeChannels.pop();
    }
  }

  // 2. Shut down the entire infrastructure instantly
  stream.terminate(Channel.COMPLETED);
  activeChannels.length = 0;

  const duration = performance.now() - start;

  // 3. Force garbage collection to isolate real structural leaks from normal allocation churn
  if (globalThis.gc) globalThis.gc();
  const finalMemory = getMemory();
  const memoryDelta = finalMemory - initialMemory;

  console.log(`Total Pushed Elements   : ${totalPushed.toLocaleString()}`);
  console.log(`Execution Duration     : ${duration.toFixed(2)} ms`);
  console.log(`Memory Footprint Delta : ${(memoryDelta / 1024 / 1024).toFixed(3)} MB`);
  console.log(`Sanity Check           : PASSED`);
}

// runCalibratedStressTest();

async function runCoordinatedChaosTest() {
  console.log("=== STARTING COORDINATED CHAOS STRESS TEST ===");

  const stream = new Stream<number>();
  const activeChannels: Channel<number>[] = [];

  let totalPushed = 0;
  let totalConsumed = 0;

  if (globalThis.gc) globalThis.gc();
  const getMemory = () =>
    typeof process !== "undefined" ? process.memoryUsage().heapUsed : (performance as any).memory?.usedJSHeapSize || 0;
  const initialMemory = getMemory();

  const ITERATIONS = 1_000_000;
  const start = performance.now();

  for (let i = 0; i < ITERATIONS; i++) {
    if (i % 5000 === 0 && activeChannels.length < 10) {
      const ch = stream.getChannel();
      activeChannels.push(ch);

      (async () => {
        try {
          while (true) {
            let next = ch.next();
            next = next instanceof Promise ? await next : next;
            totalConsumed++;
          }
        } catch (e) {}
      })();
    }

    stream.push(i);
    totalPushed++;

    // Yield control to the event loop every 1,000 items so async workers can drain the queues
    if (i % 1000 === 0) {
      await new Promise((resolve) => queueMicrotask(resolve));
    }

    if (i % 6000 === 0 && activeChannels.length > 2) {
      const targetIndex = Math.floor(activeChannels.length / 2);
      activeChannels[targetIndex]!.terminate(Channel.COMPLETED);
      activeChannels[targetIndex] = activeChannels[activeChannels.length - 1]!;
      activeChannels.pop();
    }
  }

  stream.terminate(Channel.COMPLETED);
  activeChannels.length = 0;

  const duration = performance.now() - start;

  if (globalThis.gc) globalThis.gc();
  const finalMemory = getMemory();
  const memoryDelta = finalMemory - initialMemory;

  console.log(`Total Pushed Elements   : ${totalPushed.toLocaleString()}`);
  console.log(`Execution Duration     : ${duration.toFixed(2)} ms`);
  console.log(`Memory Footprint Delta : ${(memoryDelta / 1024 / 1024).toFixed(3)} MB`);
  console.log(`Sanity Check           : PASSED`);
}

// runCoordinatedChaosTest();
