import { Source } from "./core/source";
import { Stream } from "./core/stream";
import { filter } from "./transformers/filter";
import { flat } from "./transformers/flat";
import { map } from "./transformers/map";
import { share } from "./transformers/share";

function getHeapSize(): number {
  if (globalThis.gc) {
    globalThis.gc();
  }
  return process.memoryUsage().heapUsed;
}

function runMemoryProfile() {
  const BATCH_SIZE = 5000;
  const STAGES = 100;
  const pipelines: any[] = new Array(BATCH_SIZE);

  const baseline = getHeapSize();

  for (let i = 0; i < BATCH_SIZE; i++) {
    let stream: Source<any> = new Stream<any>();

    for (let j = 0; j < STAGES; j++) {
      stream = stream.pipe(filter((v) => Boolean(v)));
    }
    pipelines[i] = stream.consume((self) => self.next()).next();
  }

  const finalHeap = getHeapSize();
  const totalAllocatedBytes = finalHeap - baseline;
  const bytesPerPipeline = totalAllocatedBytes / BATCH_SIZE;

  console.log("\n=== STREAM BENCHMARK RESULTS ===");
  console.log(`Total Batch Size:      ${BATCH_SIZE.toLocaleString()} pipelines of ${STAGES} stages`);
  console.log(`Total Heap Increase:   ${(totalAllocatedBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Average Per Pipeline:  ${Math.round(bytesPerPipeline).toLocaleString()} bytes`);
  console.log(`Average Per Stage:     ${Math.round(bytesPerPipeline / STAGES).toLocaleString()} bytes`);
  console.log("===============================\n");

  return pipelines.length;
}

// bun --expose-gc run profile.ts
runMemoryProfile();

// === STREAM BENCHMARK RESULTS ===
// Total Batch Size:      5,000 pipelines of 200 stages
// Total Heap Increase:   317.53 MB
// Average Per Pipeline:  66,591 bytes
// Average Per Stage:     333 bytes
// ===============================
