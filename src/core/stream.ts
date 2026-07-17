import { Consumer } from "./consumer";
import type { Closable, Named, NonEmptyString, Queue, Source, Transform } from "./types";
import type { Transformer } from "./transformer";

import { ScopeLinker } from "./scope-linker";
import { SourceLinker } from "./source-linker";

export class Stream<VALUE, NAME extends NonEmptyString = "root"> implements Source<VALUE>, Closable, Named<NAME> {
  #name: NAME;
  #consumers: Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>;
  #state: Stream.State;
  #sourceLinker?: SourceLinker<VALUE>;
  #scopeLinker?: ScopeLinker;
  #queueFactory?: Stream.QueueFactory<VALUE>;

  #pull: (stream: this, consumer: Consumer<VALUE, any>) => void;
  #draining: (stream: this) => void;
  #terminated: (stream: this, reason: "abort" | "complete") => void;
  #consumerJoined: (stream: this, consumer: Consumer<VALUE, any>) => void;
  #consumerLeft: (stream: this, consumer: Consumer<VALUE, any>) => void;

  #$pull?: Stream<Consumer<VALUE, any>, `${NAME}Pull`>;
  #$aborted?: Stream<void, `${NAME}Aborted`>;
  #$completed?: Stream<void, `${NAME}Completed`>;
  #$terminated?: Stream<"abort" | "complete", `${NAME}Terminated`>;
  #$draining?: Stream<void, `${NAME}Draining`>;
  #$consumerJoined?: Stream<Consumer<VALUE, any>, `${NAME}ConsumerJoined`>;
  #$consumerLeft?: Stream<Consumer<VALUE, any>, `${NAME}ConsumerLeft`>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this.#consumers = new Map();
    this.#name = options?.name ?? ("root" as NAME);
    this.#queueFactory = options?.queueFactory;
    this.#state = "active";

    this.#pull = options?.pull ?? (() => {});
    this.#draining = options?.draining ?? (() => {});
    this.#terminated = options?.terminated ?? (() => {});
    this.#consumerJoined = options?.consumerJoined ?? (() => {});
    this.#consumerLeft = options?.consumerLeft ?? (() => {});

    if (options?.source)
      this.#sourceLinker = new SourceLinker(options?.source, (_, value) => this.push(value), {
        terminated: (_, reason) => {
          reason === "abort" ? this.abort() : this.complete();
        },
      });
    if (options?.scope) {
      this.#scopeLinker = new ScopeLinker(this, options.scope);
    }
  }
  get name() {
    return this.#name;
  }
  get consumers() {
    const self = this;
    return {
      get count() {
        return self.#consumers.size;
      },
      get handlers() {
        return self.#consumers.keys();
      },
      [Symbol.iterator]() {
        return self.#consumers.values();
      },
    };
  }
  get $pull() {
    return (this.#$pull ??= new Stream({ name: `${this.name}Pull` }));
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
  get $consumerJoined() {
    return (this.#$consumerJoined ??= new Stream({ name: `${this.name}ConsumerJoined` }));
  }
  get $consumerLeft() {
    return (this.#$consumerLeft ??= new Stream({ name: `${this.name}ConsumerLeft` }));
  }
  #optimizePush(): void {
    const consumers = this.#consumers;
    switch (consumers.size) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const consumer = consumers.values().next().value!;
        this.push = (value) => consumer.push(value);
        break;
      default:
        this.push = (value) => {
          for (const consumer of consumers.values()) {
            consumer.push(value);
          }
        };
    }
  }
  push(value: VALUE): void {}

  listen<CUSTOM_NAME extends NonEmptyString = `${NAME}Consumer`>(
    handler: Consumer.Handler<VALUE, CUSTOM_NAME>,
    options?: Consumer.Options<VALUE, CUSTOM_NAME>,
  ): Consumer<VALUE, CUSTOM_NAME> {
    if (this.#consumers.has(handler)) return this.#consumers.get(handler)!;

    const consumer = new Consumer(handler, {
      ...options,
      name: options?.name ?? (`${this.#name}Consumer` as CUSTOM_NAME),
      queue: options?.queue ? options.queue : this.#queueFactory?.(),
      pull: this.#sourceLinker
        ? options?.pull
          ? (self) => (this.#sourceLinker!.next(), options.pull!(self))
          : () => this.#sourceLinker!.next()
        : options?.pull,

      terminated: (consumer, reason) => {
        this.#consumers.delete(handler);
        this.#optimizePush();
        if (this.#consumers.size === 0 && this.#state === "draining") this.#completed();

        options?.terminated?.(consumer, reason);
      },
    });

    this.#consumers.set(handler, consumer);

    this.#optimizePush();

    if (options?.ready !== false && this.#sourceLinker) this.#sourceLinker.next();
    return consumer;
  }

  abort(): void {
    this.push = this.abort = this.complete = () => {};

    this.#state = "aborted";

    for (const consumer of this.#consumers.values()) {
      consumer.abort();
    }

    this.#clean("aborted");
  }
  complete(): void {
    this.push = this.complete = () => {};
    if (this.#consumers.size) {
      this.#state = "draining";
      // this._eventsLinker.emit("drain", undefined);
    } else {
      this.#completed();
    }
    for (const consumer of this.#consumers.values()) {
      consumer.complete();
    }
  }
  #completed(): void {
    this.push = this.abort = this.complete = () => {};

    this.#state = "completed";

    // this._eventsLinker.emit("complete", undefined);
    this.#clean("completed");
  }
  #clean(reason: "aborted" | "completed"): void {
    if (reason === "aborted") {
      this.#sourceLinker?.abort();
      this.#scopeLinker?.abort();
    } else {
      this.#sourceLinker?.complete();
      this.#scopeLinker?.complete();
    }
  }
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    transform: Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    name: OUT_NAME,
    transform: Transform<this, OUT_NAME, OUT>,
  ): OUT;
  pipe<OUT_NAME extends NonEmptyString, OUT extends Transformer<this, any, OUT_NAME> | this>(
    nameOrTransform: OUT_NAME | Transform<this, OUT_NAME, OUT>,
    transform?: Transform<this, OUT_NAME, OUT>,
  ): OUT {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }
}

export namespace Stream {
  export type State = "active" | "draining" | "aborted" | "completed";
  export type Events<VALUE> = {
    drain: void;
    complete: void;
    abort: any;
    consumerJoin: Consumer<VALUE, any>;
    consumerLeft: Consumer<VALUE, any>;
  };
  export type Infos = {
    state: State;
    consumersCount: number;
  };

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    source?: Source<VALUE>;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
    pull?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    draining?: (stream: Stream<VALUE, NAME>) => void;
    terminated?: (stream: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoined?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    consumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
  };
}
