import { DefaultQueue } from "./default-queue";

export class DefaultSizedQueue<VALUE> extends DefaultQueue<VALUE> {
  constructor(
    private maxSize: number,
    private options?: DefaultSizedQueue.Options<VALUE>,
  ) {
    super();
    if (options?.values) {
      for (const value of options.values) {
        this.enqueue(value);
      }
    }
  }

  override enqueue(value: VALUE): void {
    if (this.maxSize <= 0) {
      this.options?.drop?.(this, value);
      return;
    }
    if (this.size === this.maxSize) {
      if (this.options?.dropStrategy === "newest") {
        this.options?.drop?.(this, value);
        return;
      }
      this.options?.drop?.(this, this.dequeue() as VALUE);
    }
    super.enqueue(value);
  }
}

export namespace DefaultSizedQueue {
  export type Options<VALUE> = {
    dropStrategy?: "oldest" | "newest";
    values?: Iterable<VALUE>;
    drop?: (queue: DefaultSizedQueue<VALUE>, value: VALUE) => void;
  };
}
