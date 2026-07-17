import type { NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export interface Consumer<VALUE, NAME extends NonEmptyString = "consumer"> {
  getName(): NAME;
  getState(): Consumer.State;
  getQueue(): Queue<VALUE>;
  //i used stream per event to avoid object allocation wrapper for each event emitted
  //because event streams are created once and lazily and do not affect the hot path when emitting raw values
  getAbortedStream(): Stream<void, `${NAME}Aborted`>;
  getCompletedStream(): Stream<void, `${NAME}Completed`>;
  getDrainingStream(): Stream<void, `${NAME}Draining`>;
  getTerminatedStream(): Stream<"abort" | "complete", `${NAME}Terminated`>;
  getPullStream(): Stream<void, `${NAME}Pull`>;
  canPush(): boolean;
  canNext(): boolean;
  canAbort(): boolean;
  canComplete(): boolean;

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
    let {
      name = "consumer" as NAME,
      ready = true,
      queue = new LinkedListQueue(),
      pull = () => {},
      drain = () => {},
      terminated = () => {},
    } = { ...options };
    options = undefined;

    let state: State = "active";

    let processing = false;

    let abortedStream: Stream<void, `${NAME}Aborted`> | undefined;
    let completedStream: Stream<void, `${NAME}Completed`> | undefined;
    let drainingStream: Stream<void, `${NAME}Draining`> | undefined;
    let terminatedStream: Stream<"abort" | "complete", `${NAME}Terminated`> | undefined;
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
      getAbortedStream() {
        if (!abortedStream) abortedStream = new Stream({ name: `${name}Aborted` });
        return abortedStream;
      },
      getCompletedStream() {
        if (!completedStream) completedStream = new Stream({ name: `${name}Completed` });
        return completedStream;
      },
      getDrainingStream() {
        if (!drainingStream) drainingStream = new Stream({ name: `${name}Draining` });
        return drainingStream;
      },
      getTerminatedStream() {
        if (!terminatedStream) terminatedStream = new Stream({ name: `${name}Terminated` });
        return terminatedStream;
      },
      getPullStream() {
        if (!pullStream) pullStream = new Stream({ name: `${name}Pull` });
        return pullStream;
      },
      canPush() {
        return state === "active";
      },
      canNext() {
        return state === "active" || state === "draining";
      },
      canAbort() {
        return state === "active" || state === "draining";
      },
      canComplete() {
        return state === "active";
      },
      push(value: VALUE): void {
        if (state !== "active") return;

        if (ready && !processing) {
          processing = true;
          ready = false;

          handler(consumer, value);

          processing = false;
        } else {
          queue.enqueue(value);
        }
      },
      next(): void {
        if (state === "aborted" || state === "completed") return;

        if (ready) {
          pull(consumer);
          pullStream?.push();
          return;
        }

        ready = true;

        if (processing) return;

        processing = true;
        while (ready && queue.size) {
          ready = false;

          const value = queue.dequeue() as VALUE;

          handler(consumer, value);
        }

        switch (state) {
          case "active":
            pull(consumer);
            pullStream?.push();
            break;
          case "draining":
            if (!queue.size) consumer.complete();
            break;
        }

        processing = false;
      },
      abort() {
        if (state === "aborted" || state === "completed") return;
        terminate("abort");
      },
      complete() {
        if (state !== "active") return;
        terminate("complete");
      },
    };

    return consumer;

    function terminate(reason: "abort" | "complete"): void {
      if (reason === "abort") {
        state = "aborted";
        queue.clear();
        abortedStream?.push();
      } else if (queue.size) {
        state = "draining";
        drain(consumer);
        drainingStream?.push();
        return;
      } else {
        state = "completed";
        completedStream?.push();
      }

      terminatedStream?.push(reason);
      terminatedStream?.complete();
      drainingStream?.complete();
      completedStream?.complete();
      abortedStream?.complete();
      pullStream?.complete();

      terminatedStream = drainingStream = completedStream = abortedStream = pullStream = undefined;

      terminated(consumer, reason);
      handler = () => {};
      pull = () => {};
      drain = () => {};
      terminated = () => {};
    }
  }

  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    pull?: (consumer: Consumer<VALUE, NAME>) => void;
    terminated?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
    drain?: (consumer: Consumer<VALUE, NAME>) => void;
  };
}
