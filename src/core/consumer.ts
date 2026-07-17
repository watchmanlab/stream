import type { NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export interface Consumer<VALUE, NAME extends NonEmptyString = "consumer"> {
  getName(): NAME;
  getState(): Consumer.State;
  getQueue(): Queue<VALUE>;
  //i used stream per event to avoid object allocation wrapper for each event emitted
  //because event streams are created once and lazily and do not affect the hot path when emitting raw values
  getAbortStream(): Stream<void, `${NAME}Abort`>;
  getCompleteStream(): Stream<void, `${NAME}Complete`>;
  getDrainStream(): Stream<void, `${NAME}Drain`>;
  getTerminateStream(): Stream<"abort" | "complete", `${NAME}Terminate`>;
  getPullStream(): Stream<void, `${NAME}Pull`>;

  push(value: VALUE): void;
  next(): void;
  abort(): void;
  complete(): void;
}

function push<VALUE, NAME extends NonEmptyString = "comsumer">(consumer: Consumer<VALUE, NAME>) {
  //
}

export namespace Consumer {
  export function create<VALUE, NAME extends NonEmptyString = "consumer">(
    handler: Consumer.Handler<VALUE, NAME>,
    options?: Consumer.Options<VALUE, NAME>,
  ) {
    options = { ...options };
    let state: State = "active";

    let pull = options?.pull ?? (() => {});
    const queue: Queue<VALUE> = options?.queue ? options.queue : new LinkedListQueue();
    const name = options?.name ?? ("consumer" as NAME);
    let ready = options?.ready === undefined ? true : options.ready;
    let processing = false;

    let abortStream: Stream<void, `${NAME}Abort`> | undefined;
    let completeStream: Stream<void, `${NAME}Complete`> | undefined;
    let drainStream: Stream<void, `${NAME}Drain`> | undefined;
    let terminateStream: Stream<"abort" | "complete", `${NAME}Terminate`> | undefined;
    let pullStream: Stream<void, `${NAME}Pull`> | undefined;

    const consumer: Consumer<VALUE, NAME> = {
      getName() {
        return name;
      },
      getState() {
        return state;
      },
      getQueue() {
        return queue;
      },
      getAbortStream() {
        if (!abortStream) abortStream = new Stream({ name: `${name}Abort` });
        return abortStream;
      },
      getCompleteStream() {
        if (!completeStream) completeStream = new Stream({ name: `${name}Complete` });
        return completeStream;
      },
      getDrainStream() {
        if (!drainStream) drainStream = new Stream({ name: `${name}Drain` });
        return drainStream;
      },
      getTerminateStream() {
        if (!terminateStream) terminateStream = new Stream({ name: `${name}Terminate` });
        return terminateStream;
      },
      getPullStream() {
        if (!pullStream) pullStream = new Stream({ name: `${name}Pull` });
        return pullStream;
      },
      push: options.push ? (value) => options!.push!({ ...consumer, push }, value) : push,
      next: options.next ? () => options!.next!({ ...consumer, next }) : next,
      complete: options.complete ? () => options!.complete!({ ...consumer, complete }) : complete,
      abort: options.abort ? () => options!.abort!({ ...consumer, abort }) : abort,
    };

    return consumer;

    function push(value: VALUE): void {
      if (state !== "active") return;

      if (ready && !processing) {
        processing = true;
        ready = false;

        handler(consumer, value);

        processing = false;
      } else {
        queue.enqueue(value);
      }
    }
    function next(): void {
      if (state === "aborted" || state === "completed") return;

      if (ready) return pull(consumer);

      ready = true;

      if (processing) return;

      processing = true;
      while (ready && queue.size) {
        ready = false;

        const value = queue.dequeue() as VALUE;

        handler(consumer, value);
      }

      if (state === "draining" && !queue.size) {
        complete();
      }

      processing = false;
    }

    function abort() {
      if (state === "aborted" || state === "completed") return;

      state = "aborted";

      options?.abort?.(consumer);

      abortStream?.push();

      terminate("abort");
    }
    function complete() {
      if (state !== "active") return;

      if (queue.size) {
        state = "draining";
        options?.drain?.(consumer);

        drainStream?.push();
        return;
      }
      state = "completed";

      options?.complete?.(consumer);

      completeStream?.push();

      terminate("complete");
    }

    function terminate(reason: "abort" | "complete"): void {
      ready = true;
      processing = true;
      queue.clear();

      terminateStream?.push(reason);
      terminateStream?.complete();
      drainStream?.complete();
      completeStream?.complete();
      abortStream?.complete();
      pullStream?.complete();

      terminateStream = drainStream = completeStream = abortStream = pullStream = undefined;
      handler = () => {};
      pull = () => {};

      options?.terminate?.(consumer, reason);

      options = undefined;
    }
  }

  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    push?: (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    pull?: (consumer: Consumer<VALUE, NAME>) => void;
    terminate?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
    drain?: (consumer: Consumer<VALUE, NAME>) => void;
    complete?: (consumer: Consumer<VALUE, NAME>) => void;
    abort?: (consumer: Consumer<VALUE, NAME>) => void;
  };
}
