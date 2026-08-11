import { Stream } from "./core/stream";
import { map } from "./transformers/map";
import { pump } from "./transformers/pump";
import { tap } from "./transformers/tap";

function getHeapSize(): number {
  if (globalThis.gc) {
    globalThis.gc();
  }
  return process.memoryUsage().heapUsed;
}

function runMemoryProfile() {
  const BATCH_SIZE = 5_000;
  const pipelines: any[] = new Array(BATCH_SIZE);

  console.log("Initializing baseline memory profile...");
  const baseline = getHeapSize();

  for (let i = 0; i < BATCH_SIZE; i++) {
    const rootStream = new Stream<number, "$root">();

    pipelines[i] = rootStream
      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))

      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(tap((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .pipe(map((v) => v))
      .consume((self) => self.next());

    // .pipe(pump());
  }

  const finalHeap = getHeapSize();
  const totalAllocatedBytes = finalHeap - baseline;
  const bytesPerPipeline = totalAllocatedBytes / BATCH_SIZE;

  console.log("\n=================== BENCHMARK RESULTS ===================");
  console.log(`Total Batch Size:      ${BATCH_SIZE.toLocaleString()} pipelines`);
  console.log(`Total Heap Increase:   ${(totalAllocatedBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Average Per Pipeline:  ${Math.round(bytesPerPipeline).toLocaleString()} bytes`);
  console.log(`Average Per Stage:     ${Math.round(bytesPerPipeline / 20).toLocaleString()} bytes`);
  console.log("=========================================================\n");

  return pipelines.length;
}

// bun --expose-gc run profile.ts
runMemoryProfile();

// Initializing baseline memory profile...

// =================== BENCHMARK RESULTS ===================
// Total Batch Size:      5,000 pipelines
// Total Heap Increase:   425.32 MB
// Average Per Pipeline:  89,197 bytes
// Average Per Stage:     4,460 bytes
// =========================================================
