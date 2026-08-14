import { map, Subject, tap, filter, Observable } from "rxjs";

function getHeapSize(): number {
  if (globalThis.gc) {
    globalThis.gc();
  }
  return process.memoryUsage().heapUsed;
}

function runMemoryProfile() {
  const BATCH_SIZE = 5_000;
  const STAGES = 100;
  const pipelines: any[] = new Array(BATCH_SIZE);

  console.log("Initializing baseline memory profile...");
  const baseline = getHeapSize();

  for (let i = 0; i < BATCH_SIZE; i++) {
    let stream: Observable<number> = new Subject<number>();

    for (let j = 0; j < STAGES; j++) {
      stream = stream.pipe(map((v) => v));
    }

    pipelines[i] = stream.subscribe((v) => console.log(v));
  }

  const finalHeap = getHeapSize();
  const totalAllocatedBytes = finalHeap - baseline;
  const bytesPerPipeline = totalAllocatedBytes / BATCH_SIZE;

  console.log("\n=================== BENCHMARK RESULTS ===================");
  console.log(`Total Batch Size:      ${BATCH_SIZE.toLocaleString()} pipelines`);
  console.log(`Total Heap Increase:   ${(totalAllocatedBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Average Per Pipeline:  ${Math.round(bytesPerPipeline).toLocaleString()} bytes`);
  console.log(`Average Per Stage:     ${Math.round(bytesPerPipeline / STAGES).toLocaleString()} bytes`);
  console.log("=========================================================\n");

  return pipelines.length;
}

// bun --expose-gc run profile.ts
runMemoryProfile();

// Initializing baseline memory profile...

// =================== BENCHMARK RESULTS ===================
// Total Batch Size:      5,000 pipelines
// Total Heap Increase:   108.59 MB
// Average Per Pipeline:  22,774 bytes
// Average Per Stage:     228 bytes
// =========================================================
