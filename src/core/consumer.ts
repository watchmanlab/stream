import type { Closable, Named, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Named<NAME>, Closable, Named<NAME> {
  #name: NAME;
  #state: Consumer.State;
  #queue: Queue<VALUE>;
  #ready: boolean;
  #processing = false;

  #handler: Consumer.Handler<VALUE, NAME>;
  #pull: NonNullable<Consumer.Options<VALUE, NAME>["pull"]>;
  #draining: NonNullable<Consumer.Options<VALUE, NAME>["draining"]>;
  #terminated: NonNullable<Consumer.Options<VALUE, NAME>["terminated"]>;
  #$terminated?: Stream<"abort" | "complete", `${NAME}Terminated`>;
  #$draining?: Stream<void, `${NAME}Draining`>;
  #$pull?: Stream<void, `${NAME}Pull`>;

  constructor(handler: Consumer.Handler<VALUE, NAME>, options?: Consumer.Options<VALUE, NAME>) {
    this.#name = options?.name ?? ("consumer" as NAME);
    this.#ready = options?.ready ?? true;
    this.#queue = options?.queue ?? new LinkedListQueue();
    this.#state = "active";

    this.#handler = handler;
    this.#pull = options?.pull ?? (() => {});
    this.#draining = options?.draining ?? (() => {});
    this.#terminated = options?.terminated ?? (() => {});
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
  get $draining() {
    return (this.#$draining ??= new Stream({ name: `${this.name}Draining` }));
  }
  get $terminated() {
    return (this.#$terminated ??= new Stream({ name: `${this.name}Terminated` }));
  }
  get $pull() {
    return (this.#$pull ??= new Stream({ name: `${this.name}Pull` }));
  }

  push(value: VALUE): void {
    if (this.#ready && !this.#processing) {
      this.#processing = true;
      this.#ready = false;
      this.#handler(this, value);
      this.#processing = false;
      // if (this.#ready) {
      //   this.#pull(this);
      //   this.#$pull?.push();
      // }
    } else {
      this.#queue.enqueue(value);
    }
  }

  next(): void {
    if (this.#ready) {
      this.#pull(this);
      this.#$pull?.push();
      return;
    }

    this.#ready = true;
    if (this.#processing) return;

    this.#processing = true;
    while (this.#ready && this.#queue.size) {
      this.#ready = false;
      const value = this.#queue.dequeue() as VALUE;
      this.#handler(this, value);
      this.#processing = false;
    }

    switch (this.#state) {
      case "active":
        this.#pull(this);
        this.#$pull?.push();
        break;
      case "draining":
        if (!this.#queue.size) this.terminate("complete");
        break;
    }
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.next = this.terminate = () => {};
      this.#state = "aborted";
      this.#queue.clear();
    } else if (this.#queue.size) {
      this.#state = "draining";
      this.#draining(this);
      this.#$draining?.push();
      return;
    } else {
      this.next = this.terminate = () => {};
      this.#state = "completed";
    }

    this.#$terminated?.push(reason);
    this.#$terminated?.terminate("complete");
    this.#$draining?.terminate("complete");
    this.#$pull?.terminate("complete");

    this.#$terminated = this.#$draining = this.#$pull = undefined;

    this.#terminated(this, reason);
    this.#handler = this.#pull = this.#draining = this.#terminated = () => {};
  }
}

export namespace Consumer {
  export type State = "active" | "draining" | "aborted" | "completed";
  export type AnyConsumer = Consumer<any, any>;
  export type Handler<VALUE, NAME extends NonEmptyString> = (consumer: Consumer<VALUE, NAME>, value: VALUE) => void;

  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    queue?: Queue<VALUE>;
    ready?: boolean;
    pull?: (consumer: Consumer<VALUE, NAME>) => void;
    terminated?: (consumer: Consumer<VALUE, NAME>, reason: "abort" | "complete") => void;
    draining?: (consumer: Consumer<VALUE, NAME>) => void;
  };
}
