import { map, Subject, tap, filter } from "rxjs";

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
    const rootStream = new Subject<number>();

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
      .pipe(filter((v) => Boolean(v)))

      .subscribe((v) => console.log(v));
  }

  const finalHeap = getHeapSize();
  const totalAllocatedBytes = finalHeap - baseline;
  const bytesPerPipeline = totalAllocatedBytes / BATCH_SIZE;

  console.log("\n=================== BENCHMARK RESULTS ===================");
  console.log(`Total Batch Size:      ${BATCH_SIZE.toLocaleString()} pipelines`);
  console.log(`Total Heap Increase:   ${(totalAllocatedBytes / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Average Per Pipeline:  ${Math.round(bytesPerPipeline).toLocaleString()} bytes`);
  console.log(`Average Per Stage:     ${Math.round(bytesPerPipeline / 10).toLocaleString()} bytes`);
  console.log("=========================================================\n");

  return pipelines.length;
}

// bun --expose-gc run profile.ts
runMemoryProfile();
