import type { Closable, Named, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Named<NAME>, Closable, Named<NAME> {
  #name: NAME;
  #state: Consumer.State;
  #queue: Queue<VALUE>;
  #ready: boolean;
  #processing: boolean;

  #handler: Consumer.Handler<VALUE, NAME>;
  #next: NonNullable<Consumer.Options<VALUE, NAME>["next"]>;
  #drain: NonNullable<Consumer.Options<VALUE, NAME>["drain"]>;
  #terminate: NonNullable<Consumer.Options<VALUE, NAME>["terminate"]>;
  #$terminate?: Stream<"abort" | "complete", `$${NAME}Terminate`>;
  #$drain?: Stream<void, `$${NAME}Drain`>;
  #$next?: Stream<void, `$${NAME}Next`>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    this.#name = options?.name ?? ("consumer" as NAME);
    this.#state = "active";
    this.#queue = options?.queue ?? new LinkedListQueue();
    this.#ready = options?.ready ?? true;
    this.#processing = false;

    this.#handler = handler;
    this.#next = options?.next ?? (() => {});
    this.#drain = options?.drain ?? (() => {});
    this.#terminate = options?.terminate ?? (() => {});
  }

  get name() {
    return this.#name;
  }
  get state() {
    return this.#state;
  }
  get queue() {
    return this.#queue;
  }
  get $drain() {
    return (this.#$drain ??= new Stream({ name: `$${this.name}Drain` }));
  }
  get $terminate() {
    return (this.#$terminate ??= new Stream({ name: `$${this.name}Terminate` }));
  }
  get $next() {
    return (this.#$next ??= new Stream({ name: `$${this.name}Next` }));
  }

  push(value: VALUE): void {
    if (this.#ready && !this.#processing) {
      this.#processing = true;
      this.#ready = false;
      this.#handler(this, value);
      this.#processing = false;
      if (this.#ready) this.next();
    } else {
      this.#queue.enqueue(value);
    }
  }

  next(): void {
    if (this.#ready && !this.#queue.size) {
      this.#next(this);
      this.#$next?.push();
      return;
    }

    this.#ready = true;
    if (this.#processing) return;

    this.#processing = true;
    while (this.#ready && this.#queue.size) {
      this.#ready = false;
      const value = this.#queue.dequeue() as VALUE;
      this.#handler(this, value);
    }

    switch (this.#state) {
      case "active":
        this.#next(this);
        this.#$next?.push();
        break;
      case "draining":
        if (!this.#queue.size) this.terminate("complete");
        break;
    }

    this.#processing = false;
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.next = this.terminate = () => {};
      this.#state = "aborted";
      this.#queue.clear();
    } else if (this.#queue.size) {
      this.#state = "draining";
      this.#drain(this);
      this.#$drain?.push();
      return;
    } else {
      this.next = this.terminate = () => {};
      this.#state = "completed";
    }

    this.#$terminate?.push(reason);
    this.#$terminate?.terminate("complete");
    this.#$drain?.terminate("complete");
    this.#$next?.terminate("complete");

    this.#$terminate = this.#$drain = this.#$next = undefined;

    this.#terminate(this, reason);
    this.#handler = this.#next = this.#drain = this.#terminate = () => {};
  }
}

export namespace Consumer {
  export function create<VALUE, NAME extends NonEmptyString = "consumer">(
    handler: Handler<VALUE, NAME>,
    options?: Options<VALUE, NAME>,
  ) {
    const name = options?.name ?? ("consumer" as NAME);
    const queue = options?.queue ?? new LinkedListQueue();
    let state = "active";
    let ready = options?.ready ?? true;
    let processing = false;

    let next = options?.next ?? (() => {});
    let drain = options?.drain ?? (() => {});
    let terminate = options?.terminate ?? (() => {});

    let $terminate: Stream<"abort" | "complete", `$${NAME}Terminate`> | undefined;
    let $drain: Stream<void, `$${NAME}Drain`> | undefined;
    let $next: Stream<void, `$${NAME}Next`> | undefined;

    const consumer = {
      get name() {
        return name;
      },
      get state() {
        return state;
      },
      get queue() {
        return queue;
      },
      get $drain() {
        return ($drain ??= new Stream({ name: `$${this.name}Drain` }));
      },
      get $terminate() {
        return ($terminate ??= new Stream({ name: `$${this.name}Terminate` }));
      },
      get $next() {
        return ($next ??= new Stream({ name: `$${this.name}Next` }));
      },
      push(value: VALUE): void {
        if (ready && !processing) {
          processing = true;
          ready = false;
          handler(consumer, value);
          processing = false;
          if (ready) consumer.next();
        } else {
          queue.enqueue(value);
        }
      },
      next(): void {
        if (ready && !queue.size) {
          next(consumer);
          $next?.push();
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
            next(consumer);
            $next?.push();
            break;
          case "draining":
            if (!queue.size) consumer.terminate("complete");
            break;
        }

        processing = false;
      },
      terminate(reason: "abort" | "complete"): void {
        consumer.push = () => {};
        if (reason === "abort") {
          consumer.next = consumer.terminate = () => {};
          state = "aborted";
          queue.clear();
        } else if (queue.size) {
          state = "draining";
          drain(consumer);
          $drain?.push();
          return;
        } else {
          consumer.next = consumer.terminate = () => {};
          state = "completed";
        }

        $terminate?.push(reason);
        $terminate?.terminate("complete");
        $drain?.terminate("complete");
        $next?.terminate("complete");

        $terminate = $drain = $next = undefined;

        terminate(consumer, reason);
        handler = next = drain = terminate = () => {};
      },
    } as Consumer<VALUE, NAME>;

    return consumer;
  }
  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    next?: (consumer: Consumer<VALUE, NAME>) => void;
    drain?: (consumer: Consumer<VALUE, NAME>) => void;
    terminate?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
  };
}
