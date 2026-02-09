import { Stream } from "../../stream-0";
import { WorkerPool } from "../../workerPool/workerPool";
import { concurrent } from "../concurrent/concurrent";

export function parallel<VALUE, MAPPED, ARGS extends any[]>(
  mapper: parallel.Mapper<VALUE, MAPPED, ARGS>,
  ...args: ARGS
): Stream.Transformer<Stream<VALUE>, Stream<MAPPED>> {
  if (typeof Worker === "undefined") return concurrent(mapper as unknown as concurrent.Mapper<VALUE, MAPPED>);

  return (stream) => {
    const { execute } = WorkerPool.register(mapper, args);
    return stream.pipe(concurrent(async (value) => await execute(value)));
  };
}

export namespace parallel {
  export type Mapper<VALUE, MAPPED, ARGS extends any[]> = (value: VALUE, ...args: ARGS) => MAPPED | Promise<MAPPED>;
}
