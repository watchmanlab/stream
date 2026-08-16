import { map, Subject, Observable, mergeMap, filter, share } from "rxjs";

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
    let subject: Observable<any> = new Subject<any>();

    for (let j = 0; j < STAGES; j++) {
      subject = subject.pipe(map((v) => v));
    }

    pipelines[i] = subject.subscribe((v) => console.log(v));
  }

  const finalHeap = getHeapSize();
  const totalAllocatedBytes = finalHeap - baseline;
  const bytesPerPipeline = totalAllocatedBytes / BATCH_SIZE;

  console.log("\n=== RXJS BENCHMARK RESULTS ===");
  console.log(`Total Batch Size:      ${BATCH_SIZE.toLocaleString()} pipelines of ${STAGES} stages`);
  console.log(`Total Heap Increase:   ${(totalAllocatedBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Average Per Pipeline:  ${Math.round(bytesPerPipeline).toLocaleString()} bytes`);
  console.log(`Average Per Stage:     ${Math.round(bytesPerPipeline / STAGES).toLocaleString()} bytes`);
  console.log("===============================\n");

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
