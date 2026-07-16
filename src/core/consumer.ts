import type { NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export interface Consumer<VALUE, NAME extends NonEmptyString = "consumer"> {
  getName(): NAME;
  getState(): Consumer.State;
  getQueue(): Queue<VALUE>;
  isReady(): boolean;
  isProcessing(): boolean;
  //i used stream per event to avoid object allocation wrapper for each event emitted
  //because event streams are created once and lazily and do not affect the hot path when emitting raw values
  getTerminatedStream(): Stream<"aborted" | "completed", `${NAME}Terminated`>;
  getPullStream(): Stream<Consumer<VALUE, NAME>, `${NAME}Pull`>;

  push(value: VALUE): void;
  next(): void;
  terminate(reason: "abort" | "complete"): void;
}

export namespace Consumer {
  export function create<VALUE, NAME extends NonEmptyString = "consumer">(
    handler: Consumer.Handler<VALUE, NAME>,
    options?: Consumer.Options<VALUE, NAME>,
  ) {
    options = { ...options };
    let pull = options?.pull ?? (() => {});
    const queue: Queue<VALUE> = options?.queue ? options.queue : new LinkedListQueue();
    const name = options?.name ?? ("consumer" as NAME);
    let ready = options?.ready === undefined ? true : options.ready;
    let processing = false;
    let state: State = "active";

    let terminatedStream: Stream<"aborted" | "completed", `${NAME}Terminated`> | undefined;
    let pullStream: Stream<Consumer<VALUE, NAME>, `${NAME}Pull`> | undefined;

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
      isReady() {
        return ready;
      },
      isProcessing() {
        return processing;
      },
      getTerminatedStream() {
        if (!terminatedStream) terminatedStream = new Stream({ name: `${consumer.getName()}Terminated` });
        return terminatedStream;
      },
      getPullStream() {
        if (!pullStream) pullStream = new Stream({ name: `${consumer.getName()}Pull` });
        return pullStream;
      },
      push: options.push ? (value: VALUE) => options.push!({ ...consumer, push }, value) : push,
      next: options.next ? () => options.next!({ ...consumer, next }) : next,
      terminate: options.terminate ? (reason) => options.terminate!({ ...consumer, terminate }, reason) : terminate,
    };

    return consumer;

    function push(value: VALUE): void {
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

      if (processing) {
        return;
      } else if (!queue.size) {
        if (state === "active") {
          pull();
        } else {
          completed();
        }
      } else {
        drain();
      }
    }
    function terminate(reason: "abort" | "complete"): void {
      if (reason === "complete") {
        consumer.push = () => {};

        if (queue.size) {
          state = "drain";
        } else {
          completed();
        }
      } else {
        consumer.push = consumer.next = consumer.terminate = pull = handler = () => {};

        state = "aborted";
        ready = true;
        processing = false;
        queue.clear();
        terminatedStream?.push(state);
      }
    }
    function drain(): void {
      processing = true;
      while (ready && queue.size) {
        ready = false;

        const value = queue.dequeue() as VALUE;
        handler(consumer, value);
      }
      processing = false;
    }
    function completed(): void {
      consumer.next = consumer.terminate = pull = handler = () => {};
      state = "completed";
      ready = true;
      processing = false;
      terminatedStream?.push(state);
    }
  }
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    pull?: () => void;
    push?: (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    terminate?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
  };
}
