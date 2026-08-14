import { DefaultQueue } from "./default-queue";

export class SizedDefaultQueue<VALUE> extends DefaultQueue<VALUE> {
  constructor(
    private maxSize: number,
    values?: Iterable<VALUE>,
  ) {
    super();
    if (values) {
      for (const value of values) {
        this.enqueue(value);
      }
    }
  }

  override enqueue(value: VALUE): void {
    if (this.size === this.maxSize) this.dequeue();
    super.enqueue(value);
  }
}
