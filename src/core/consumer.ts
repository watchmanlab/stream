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
  getPullStream(): Stream<void, `${NAME}Pull`>;

  push(value: VALUE): void;
  next(): void;
  abort(): void;
  complete(): void;
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
        if (!abortStream) abortStream = new Stream({ name: `${consumer.getName()}Abort` });
        return abortStream;
      },
      getCompleteStream() {
        if (!completeStream) completeStream = new Stream({ name: `${consumer.getName()}Complete` });
        return completeStream;
      },
      getPullStream() {
        if (!pullStream) pullStream = new Stream({ name: `${consumer.getName()}Pull` });
        return pullStream;
      },
      push: options.push ? (value: VALUE) => options.push!({ ...consumer, push }, value) : push,
      next: options.next ? () => options.next!({ ...consumer, next }) : next,
      abort: options.abort ? () => options.abort!({ ...consumer, abort }) : abort,
      complete: options.complete ? () => options.complete!({ ...consumer, complete }) : complete,
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
      if (ready) return pull();

      ready = true;

      if (processing) return;

      processing = true;
      while (ready && queue.size) {
        ready = false;

        const value = queue.dequeue() as VALUE;
        handler(consumer, value);
      }
      if (state === "draining") dispose("completed");
      processing = false;
    }
    function abort(): void {
      if (state === "aborted" || state === "completed") return;
      dispose("aborted");
    }
    function complete(): void {
      if (state !== "active") return;

      if (queue.size) {
        state = "draining";
      } else {
        dispose("completed");
      }
    }
    function dispose(reason: "aborted" | "completed"): void {
      state = reason;
      ready = false;
      processing = true;
      queue.clear();

      if (reason == "aborted") {
        abortStream?.push();
        abortStream?.complete();
        completeStream?.abort();
        pullStream?.abort();
      } else {
        completeStream?.push();
        abortStream?.abort();
        completeStream?.complete();
        pullStream?.complete();
      }

      abortStream?.complete();
      completeStream?.complete();

      abortStream = completeStream = undefined;
    }
  }

  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    pull?: () => void;
    dispose?: (consumer: Consumer<VALUE, NAME>, reason: "aborted" | "completed") => void;
    push?: (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    abort?: (consumer: Consumer<VALUE, NAME>) => void;
    complete?: (consumer: Consumer<VALUE, NAME>) => void;
  };
}
