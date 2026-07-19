import { Consumer } from "./consumer";
import type { Closable, Named, NonEmptyString, Queue, Source, Transform } from "./types";
import type { Transformer } from "./transformer";

import { ScopeLinker } from "./scope-linker";

export class Stream<VALUE, NAME extends NonEmptyString = "root"> implements Source<VALUE>, Closable, Named<NAME> {
  #name: NAME;
  #consumers: Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>;
  #state: Stream.State;
  #scopeLinker?: ScopeLinker;
  #queueFactory?: Stream.QueueFactory<VALUE>;

  #next: NonNullable<Stream.Options<VALUE, NAME>["next"]>;
  #drain: NonNullable<Stream.Options<VALUE, NAME>["drain"]>;
  #terminate: NonNullable<Stream.Options<VALUE, NAME>["terminate"]>;
  #consumerJoin: NonNullable<Stream.Options<VALUE, NAME>["consumerJoin"]>;
  #consumerLeft: NonNullable<Stream.Options<VALUE, NAME>["consumerLeft"]>;

  #$next?: Stream<Consumer<VALUE, any>, `${NAME}Next`>;
  #$terminate?: Stream<"abort" | "complete", `$${NAME}Terminate`>;
  #$drain?: Stream<void, `${NAME}Drain`>;
  #$consumerJoin?: Stream<Consumer<VALUE, any>, `$${NAME}ConsumerJoin`>;
  #$consumerLeft?: Stream<Consumer<VALUE, any>, `$${NAME}ConsumerLeft`>;

  #pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this.#consumers = new Map();
    this.#name = options?.name ?? ("root" as NAME);
    this.#queueFactory = options?.queueFactory;
    this.#state = "active";
    this.#pulling = false;

    this.#next = options?.next ?? (() => {});
    this.#drain = options?.drain ?? (() => {});
    this.#terminate = options?.terminate ?? (() => {});
    this.#consumerJoin = options?.consumerJoin ?? (() => {});
    this.#consumerLeft = options?.consumerLeft ?? (() => {});

    this.#scopeLinker = options?.scope ? new ScopeLinker(this, options.scope) : undefined;
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
  get $next() {
    return (this.#$next ??= new Stream({ name: `${this.name}Next` }));
  }
  get $drain() {
    return (this.#$drain ??= new Stream({ name: `${this.name}Drain` }));
  }
  get $terminate() {
    return (this.#$terminate ??= new Stream({ name: `$${this.name}Terminate` }));
  }
  get $consumerJoin() {
    return (this.#$consumerJoin ??= new Stream({ name: `$${this.name}ConsumerJoin` }));
  }
  get $consumerLeft() {
    return (this.#$consumerLeft ??= new Stream({ name: `$${this.name}ConsumerLeft` }));
  }
  #optimizePush(): void {
    const consumers = this.#consumers;
    switch (consumers.size) {
      case 0:
        this.push = () => (this.#pulling = false);
        break;
      case 1:
        const consumer = consumers.values().next().value!;
        this.push = (value) => {
          this.#pulling = false;
          consumer.push(value);
        };
        break;
      default:
        this.push = (value) => {
          this.#pulling = false;
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
      next: (consumer) => {
        if (this.#pulling) return;
        this.#pulling = true;
        options?.next?.(consumer);
        this.#next(this, consumer);
      },

      terminate: (consumer, reason) => {
        this.#consumers.delete(handler);
        this.#optimizePush();
        this.#consumerLeft(this, consumer);
        this.#$consumerLeft?.push(consumer);
        if (this.#consumers.size === 0 && this.#state === "drain") this.terminate("complete");

        options?.terminate?.(consumer, reason);
      },
    });

    this.#consumers.set(handler, consumer);
    this.#optimizePush();
    this.#consumerJoin(this, consumer);
    this.#$consumerJoin?.push(consumer);

    if (options?.ready !== false && !this.#pulling) {
      this.#pulling = true;
      options?.next?.(consumer);
      this.#next(this, consumer);
    }
    return consumer;
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.terminate = () => {};
      this.#state = "aborted";
    } else if (this.#consumers.size) {
      this.#state = "drain";
      this.#drain(this);
      this.#$drain?.push();
      return;
    } else {
      this.terminate = () => {};
      this.#state = "completed";
    }

    for (const consumer of this.#consumers.values()) {
      consumer.terminate(reason);
    }
    this.#terminate(this, reason);
    this.#$terminate?.push(reason);

    this.#scopeLinker?.terminate(reason);
    this.#scopeLinker = undefined;

    this.#$terminate =
      this.#$drain =
      this.#$next =
      this.#$consumerJoin =
      this.#$consumerLeft =
      this.#scopeLinker =
      this.#queueFactory =
        undefined;
    this.#next = this.#drain = this.#terminate = this.#consumerJoin = this.#consumerLeft = () => {};
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
  export type State = "active" | "drain" | "aborted" | "completed";

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
    next?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    drain?: (stream: Stream<VALUE, NAME>) => void;
    terminate?: (stream: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoin?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    consumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
  };
}
