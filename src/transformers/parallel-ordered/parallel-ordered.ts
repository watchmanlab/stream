import { Stream } from "../../stream-0";
import { WorkerPool } from "../../workerPool/workerPool";
import { concurrentOrdered } from "../concurrent-ordered/concurrent-ordered";

export function parallelOrdered<VALUE, MAPPED, ARGS extends any[]>(
  mapper: parallelOrdered.Mapper<VALUE, MAPPED, ARGS>,
  ...args: ARGS
): Stream.Transformer<Stream<VALUE>, Stream<MAPPED>> {
  if (typeof Worker === "undefined")
    return concurrentOrdered(mapper as unknown as concurrentOrdered.Mapper<VALUE, MAPPED>);

  return (stream) => {
    const { execute } = WorkerPool.register(mapper, ...args);
    return stream.pipe(concurrentOrdered(async (value) => await execute(value)));
  };
}

export namespace parallelOrdered {
  export type Mapper<VALUE, MAPPED, ARGS extends any[]> = (value: VALUE, ...args: ARGS) => MAPPED | Promise<MAPPED>;
}
