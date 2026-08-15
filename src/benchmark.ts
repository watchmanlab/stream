import { Stream } from "./core/stream.js";
import { AnyStream } from "./core/types.js";

function mapStage(index: number) {
  return (input: AnyStream) => {
    const nextStream = new Stream({ name: `$stage_${index}` });

    // Pure, unbroken synchronous link
    const upstreamConsumer = input.consume((self, value) => {
      nextStream.push(value);
      self.next();
    });
    upstreamConsumer.next();

    return nextStream;
  };
}

async function runBenchmark() {
  const STAGES = 300; // Deep synchronous topology
  const iterations = 1000000;

  console.log("=========================================");
  console.log("⚡ SYNC MEGA-PIPELINE ENGINE BENCHMARK ⚡");
  console.log("=========================================");
  console.log(`Building a massive ${STAGES}-stage synchronous graph...`);

  let sourceStream = new Stream({ name: "$source" });
  let current = sourceStream;

  for (let i = 1; i <= STAGES; i++) {
    current = current.pipe(mapStage(i) as any);
  }

  let processedCount = 0;
  const sink = current.consume((self, value) => {
    processedCount++;
    self.next();
  });
  sink.next();

  console.log("Warming up V8 JIT Compiler...");
  for (let i = 0; i < 5000; i++) {
    sourceStream.push(i);
  }
  processedCount = 0;

  console.log(`Streaming ${iterations.toLocaleString()} values through ${STAGES} stages...`);

  const startTime = process.hrtime.bigint();

  for (let i = 0; i < iterations; i++) {
    sourceStream.push(i);
  }

  const endTime = process.hrtime.bigint();

  const totalNanoseconds = Number(endTime - startTime);
  const totalMilliseconds = totalNanoseconds / 1_000_000;
  const totalSeconds = totalNanoseconds / 1_000_000_000;

  const totalPipelineOperations = iterations * STAGES;
  const throughputOpsPerSec = totalPipelineOperations / totalSeconds;

  console.log("\n---------------- RESULTS ----------------");
  console.log(`Total Time        : ${totalMilliseconds.toFixed(2)} ms`);
  console.log(`Processed Elements: ${processedCount.toLocaleString()} items`);
  console.log(`Total Stage Pushes: ${totalPipelineOperations.toLocaleString()} operations`);
  console.log(`Engine Velocity   : \x1b[32m${Math.round(throughputOpsPerSec).toLocaleString()} ops/sec\x1b[0m`);
  console.log("=========================================\n");
}

runBenchmark().catch(console.error);
