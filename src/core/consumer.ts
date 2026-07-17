import type { Closable, Named, NonEmptyString, Queue } from "./types";
import { LinkedListQueue } from "./linked-list-queue";
import { Stream } from "./stream";

export class Consumer<VALUE, NAME extends NonEmptyString = "consumer"> implements Named<NAME>, Closable {
  #name: NAME;
  #state: Consumer.State;
  #queue: Queue<VALUE>;
  #ready: boolean;
  #processing = false;

  #handler: Consumer.Handler<VALUE, NAME>;
  #pull: (consumer: this) => void;
  #draining: (consumer: this) => void;
  #terminated: (consumer: this, reason: "abort" | "complete") => void;

  #$aborted?: Stream<void, `${NAME}Aborted`>;
  #$completed?: Stream<void, `${NAME}Completed`>;
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
  get $aborted() {
    return (this.#$aborted ??= new Stream({ name: `${this.name}Aborted` }));
  }
  get $completed() {
    return (this.#$completed ??= new Stream({ name: `${this.name}Completed` }));
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
    if (this.#state !== "active") return;

    if (this.#ready && !this.#processing) {
      this.#processing = true;
      this.#ready = false;
      this.#handler(this, value);
      this.#processing = false;
    } else {
      this.#queue.enqueue(value);
    }
  }

  next(): void {
    if (this.#state === "aborted" || this.#state === "completed") return;

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
    }

    switch (this.#state) {
      case "active":
        this.#pull(this);
        this.#$pull?.push();
        break;
      case "draining":
        if (!this.#queue.size) this.complete();
        break;
    }

    this.#processing = false;
  }

  abort() {
    if (this.#state === "aborted" || this.#state === "completed") return;
    this.#terminate("abort");
  }

  complete() {
    if (this.#state !== "active") return;
    this.#terminate("complete");
  }

  #terminate(reason: "abort" | "complete"): void {
    if (reason === "abort") {
      this.#state = "aborted";
      this.#queue.clear();
      this.#$aborted?.push();
    } else if (this.#queue.size) {
      this.#state = "draining";
      this.#draining(this);
      this.#$draining?.push();
      return;
    } else {
      this.#state = "completed";
      this.#$completed?.push();
    }

    this.#$terminated?.push(reason);
    this.#$terminated?.complete();
    this.#$draining?.complete();
    this.#$completed?.complete();
    this.#$aborted?.complete();
    this.#$pull?.complete();

    this.#$terminated = this.#$draining = this.#$completed = this.#$aborted = this.#$pull = undefined;

    this.#terminated(this, reason);

    this.#handler = () => {};
    this.#pull = () => {};
    this.#draining = () => {};
    this.#terminated = () => {};
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
