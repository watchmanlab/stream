import { Consumer } from "./consumer";
import type { Closable, Named, NonEmptyString, Queue, Source, Transform } from "./types";
import type { Transformer } from "./transformer";

import { ScopeLinker } from "./scope-linker";
import { SourceLinker } from "./source-linker";

export class Stream<VALUE, NAME extends NonEmptyString = "root"> implements Source<VALUE>, Closable, Named<NAME> {
  #name: NAME;
  #consumers: Map<Consumer.Handler<VALUE, any>, Consumer<VALUE, any>>;
  #state: Stream.State;
  #scopeLinker?: ScopeLinker;
  #queueFactory?: Stream.QueueFactory<VALUE>;

  #pull: NonNullable<Stream.Options<VALUE, NAME>["pull"]>;
  #draining: NonNullable<Stream.Options<VALUE, NAME>["draining"]>;
  #terminated: NonNullable<Stream.Options<VALUE, NAME>["terminated"]>;
  #consumerJoined: NonNullable<Stream.Options<VALUE, NAME>["consumerJoined"]>;
  #consumerLeft: NonNullable<Stream.Options<VALUE, NAME>["consumerLeft"]>;

  #$pull?: Stream<Consumer<VALUE, any>, `${NAME}Pull`>;
  #$terminated?: Stream<"abort" | "complete", `${NAME}Terminated`>;
  #$draining?: Stream<void, `${NAME}Draining`>;
  #$consumerJoined?: Stream<Consumer<VALUE, any>, `${NAME}ConsumerJoined`>;
  #$consumerLeft?: Stream<Consumer<VALUE, any>, `${NAME}ConsumerLeft`>;

  #pulling: boolean;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this.#consumers = new Map();
    this.#name = options?.name ?? ("root" as NAME);
    this.#queueFactory = options?.queueFactory;
    this.#state = "active";
    this.#pulling = false;

    this.#pull = options?.pull ?? (() => {});
    this.#draining = options?.draining ?? (() => {});
    this.#terminated = options?.terminated ?? (() => {});
    this.#consumerJoined = options?.consumerJoined ?? (() => {});
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
  get $pull() {
    return (this.#$pull ??= new Stream({ name: `${this.name}Pull` }));
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
      pull: (consumer) => {
        if (this.#pulling) return;
        this.#pulling = true;
        options?.pull?.(consumer);
        this.#pull(this, consumer);
      },

      terminated: (consumer, reason) => {
        this.#consumers.delete(handler);
        this.#optimizePush();
        this.#consumerLeft(this, consumer);
        this.#$consumerLeft?.push(consumer);
        if (this.#consumers.size === 0 && this.#state === "draining") this.terminate("complete");

        options?.terminated?.(consumer, reason);
      },
    });

    this.#consumers.set(handler, consumer);
    this.#optimizePush();
    this.#consumerJoined(this, consumer);
    this.#$consumerJoined?.push(consumer);

    if (options?.ready !== false && !this.#pulling) {
      this.#pull(this, consumer);
    }
    return consumer;
  }

  terminate(reason: "abort" | "complete"): void {
    this.push = () => {};
    if (reason === "abort") {
      this.terminate = () => {};
      this.#state = "aborted";
    } else if (this.#consumers.size) {
      this.#state = "draining";
      this.#draining(this);
      this.#$draining?.push();
      return;
    } else {
      this.terminate = () => {};
      this.#state = "completed";
    }

    for (const consumer of this.#consumers.values()) {
      consumer.terminate(reason);
    }
    this.#terminated(this, reason);
    this.#$terminated?.push(reason);

    this.#scopeLinker?.terminate(reason);
    this.#scopeLinker = undefined;

    this.#$terminated =
      this.#$draining =
      this.#$pull =
      this.#$consumerJoined =
      this.#$consumerLeft =
      this.#scopeLinker =
      this.#queueFactory =
        undefined;
    this.#pull = this.#draining = this.#terminated = this.#consumerJoined = this.#consumerLeft = () => {};
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

  export type QueueFactory<VALUE> = () => Queue<VALUE>;
  export type Options<VALUE, NAME extends NonEmptyString> = {
    name?: NAME;
    scope?: ScopeLinker.Scope;
    queueFactory?: QueueFactory<VALUE>;
    pull?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    draining?: (stream: Stream<VALUE, NAME>) => void;
    terminated?: (stream: Stream<VALUE, NAME>, reason: "abort" | "complete") => void;
    consumerJoined?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
    consumerLeft?: (stream: Stream<VALUE, NAME>, consumer: Consumer<VALUE, any>) => void;
  };
}
