export class Stream<VALUE> implements AsyncIterable<VALUE> {
  protected consumers = new Map<VALUE[], Stream.Resolver>();
  protected source: Stream.Source<VALUE> | undefined;

  constructor(source?: Stream.Source<VALUE>) {
    this.setSource(source);
  }

  push(value: VALUE) {
    for (const [queue, resolver] of this.consumers) {
      queue.unshift(value);
      resolver();
    }
  }
  protected requestingNext = false;
  async *[Symbol.asyncIterator]() {
    const iterator = this.source
      ? Symbol.asyncIterator in this.source
        ? this.source[Symbol.asyncIterator]()
        : this.source()
      : undefined;

    const queue: VALUE[] = [];

    try {
      while (true) {
        if (queue.length) {
          yield queue.pop()!;
        } else {
          if (!this.requestingNext) {
            this.requestingNext = true;
            iterator?.next().then((result) => {
              if (result.done) return;
              this.push(result.value);
              this.requestingNext = false;
            });
          }
          await new Promise<void>((resolve) => {
            this.consumers.set(queue, resolve);
          });
        }
      }
    } finally {
      this.consumers.delete(queue);
      queue.length = 0;
      iterator?.return?.();
      return;
    }
  }

  getSource() {
    return this.source;
  }
  setSource(source?: Stream.Source<VALUE>) {
    this.source = source;
  }
}

export namespace Stream {
  //     export class Controller extends Stream<void> {
  //     protected _aborted = false;
  //     protected _signals: Set<Stream<any>> | undefined;
  //     protected _cleanups: Set<Controller.Cleanup> | undefined;

  //     constructor(cleanup?: Controller.Cleanup) {
  //       super();
  //       if (cleanup) this.addCleanup(cleanup);
  //     }
  //     get aborted() {
  //       return this._aborted;
  //     }
  //     get signals() {
  //       return this._signals && [...this._signals];
  //     }
  //     abort(): void {
  //       if (this._aborted) return;
  //       this._aborted = true;
  //       this._signals = undefined;
  //       this._cleanups?.forEach((cleanup) => cleanup());
  //       this._cleanups = undefined;
  //       this.push();
  //     }
  //     addCleanup(cleanup: Controller.Cleanup) {
  //       this._cleanups ? this._cleanups.add(cleanup) : (this._cleanups = new Set([cleanup]));
  //       return this;
  //     }
  //     removeCleanup(cleanup: Controller.Cleanup) {
  //       this._cleanups?.delete(cleanup);
  //       if (!this._cleanups?.size) this._cleanups = undefined;
  //       return this;
  //     }
  //     addSignal(signal: Stream<any>) {
  //       this._signals ? this._signals.add(signal) : (this._signals = new Set([signal]));

  //       signal.listen((_, controller) => {
  //         if (this._signals?.has(signal)) this.abort();
  //         controller.abort();
  //       });
  //       return this;
  //     }
  //     removeSignal(signal: Stream<any>) {
  //       this._signals?.delete(signal);
  //       if (!this._signals?.size) this._signals = undefined;

  //       return this;
  //     }

  //     [Symbol.dispose](): void {
  //       this.abort();
  //       super[Symbol.dispose]();
  //     }
  //     static abort(controllers: Controller[]) {
  //       controllers.forEach((controller) => controller.abort());
  //     }
  //     static ABORTED = Symbol("aborted");
  //   }
  //   export namespace Controller {
  //     export type Cleanup = () => void;
  //     export type Aborted = typeof Controller.ABORTED;
  //   }
  export type Resolver = () => void;
  export type GeneratorFunction<VALUE> = () => AsyncGenerator<VALUE>;
  export type Source<VALUE> = GeneratorFunction<VALUE> | AsyncIterable<VALUE>;
}
