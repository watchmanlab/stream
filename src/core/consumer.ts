import type { NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export interface Consumer<VALUE, NAME extends NonEmptyString = "consumer"> {
  getName(): NAME;
  getState(): Consumer.State;
  getQueue(): Queue.Iterator<VALUE>;
  getReady(): boolean;
  getProcessing(): boolean;
  //i used stream per event to avoid object allocation wrapper for each event emitted
  //because event streams are created once and lazily and do not affect the hot path when emitting raw values
  get$terminated(): Stream<Consumer<VALUE, NAME>, `${NAME}Terminated`>;

  push(value: VALUE): void;
  next(): void;
  terminate(drain: boolean): void;
}

export namespace Consumer {
  export function create<VALUE, NAME extends NonEmptyString = "consumer">(
    handler: Consumer.Handler<VALUE, NAME>,
    options?: Consumer.Options<VALUE, NAME>,
  ) {
    options = { ...options };
    let next = options?.next ?? (() => {});
    const queue: Queue<VALUE> = options?.queue ? options.queue : new LinkedListQueue();
    let ready = options?.ready === undefined ? true : options.ready;
    let processing = false;
    let state: State = "active";

    let $terminated: Stream<Consumer<VALUE, NAME>, `${NAME}Terminated`> | undefined;

    const consumer: Consumer<VALUE, NAME> = {
      getName() {
        return options?.name ?? ("consumer" as NAME);
      },
      getState() {
        return state;
      },
      getQueue() {
        return queue.values();
      },
      getReady() {
        return ready;
      },
      getProcessing() {
        return processing;
      },
      get$terminated() {
        if (!$terminated) $terminated = new Stream({ name: `${consumer.getName()}Terminated` });
        return $terminated;
      },
      push: options.push ? (value: VALUE) => options.push!({ ...consumer, push }, value) : push,
      next(): void {
        if (ready) return next(consumer);

        ready = true;

        if (!processing && queue.size === 0) {
          if (state === "active") {
            next(consumer);
          } else {
            completed();
          }
        } else {
          drain();
        }
      },
      terminate(drain) {
        if (drain) {
          consumer.push = () => {};

          if (queue.size) {
            state = "drain";
          } else {
            completed();
          }
        } else {
          consumer.push = consumer.next = consumer.terminate = next = handler = () => {};

          state = "aborted";
          ready = true;
          processing = false;
          queue.clear();
          $terminated?.push(consumer);
        }
      },
    };

    return consumer;

    function push(value: VALUE): void {
      if (ready && !processing) {
        processing = true;
        ready = false;

        handler(consumer, value);

        processing = false;
        if (ready) next(consumer);
      } else {
        queue.enqueue(value);
      }
    }

    function drain(): void {
      if (processing) return;

      processing = true;
      while (ready && queue.size) {
        ready = false;

        const value = queue.dequeue() as VALUE;

        handler(consumer, value);
      }
      processing = false;
    }
    function completed(): void {
      consumer.next = consumer.terminate = next = handler = () => {};
      state = "completed";
      ready = true;
      processing = false;
      $terminated?.push(consumer);
    }
  }
  export type State = "active" | "drain" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    push?: (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    complete?: (consumer: Consumer<VALUE, NAME>) => void;
    abort?: (consumer: Consumer<VALUE, NAME>) => void;
  };
}
