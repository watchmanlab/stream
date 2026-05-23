import { Consumer } from "./consumer.ts";

export class Consumers<VALUE> implements Iterable<Consumer<VALUE>>, Disposable {
  private _collection = new Set<Consumer<VALUE>>();

  constructor(private options?: Consumers.Options<VALUE>) {}
  [Symbol.iterator](): SetIterator<Consumer<VALUE>> {
    return this._collection.values();
  }
  [Symbol.dispose](): void {
    this.clear();
  }
  get(options: Consumer.Options<VALUE>): Consumer<VALUE> {
    const consumer = new Consumer<VALUE>({
      ...options,
      return: () => {
        this._collection.delete(consumer);
        this.options?.detach?.(consumer);
        options?.return?.();
      },
      pull: () => {
        this.options?.pull?.();
        options?.pull?.();
      },
    });
    this._collection.add(consumer);
    this.options?.attach?.(consumer);
    return consumer;
  }
  clear(): void {
    const collection = [...this];

    for (const consumer of this) {
      consumer.return();
    }

    this.options?.clear?.(collection);
  }
  get count() {
    return this._collection.size;
  }
}

export namespace Consumers {
  export type Options<VALUE> = {
    attach?: (consumer: Consumer<VALUE>) => void;
    detach?: (consumer: Consumer<VALUE>) => void;
    clear?: (consumers: Consumer<VALUE>[]) => void;
    pull?: () => void;
  };
}
