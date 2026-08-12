import { Consumer } from "./consumer";
import { ConsumerSet } from "./types";
import { EMPTY_FUNCTION } from "./consts";

export class ArrayConsumerSet<VALUE> implements ConsumerSet<VALUE> {
  private _consumers?: Consumer<VALUE> | Consumer<VALUE>[];

  get size(): number {
    return this._consumers instanceof Consumer ? 1 : (this._consumers?.length ?? 0);
  }

  private _optimizePush(): void {
    if (Array.isArray(this._consumers)) {
      const snapshot = Array.from(this._consumers);
      const length = snapshot.length;
      this.push = (value) => {
        for (let i = 0; i < length; i++) {
          snapshot[i].push(value);
        }
      };
    } else if (this._consumers) {
      const consumer = this._consumers;
      this.push = (value) => consumer.push(value);
    } else {
      this.push = EMPTY_FUNCTION;
    }
  }
  push(value: VALUE) {}

  add(consumer: Consumer<VALUE>): void {
    if (!this._consumers) {
      this._consumers = consumer;
    } else if (this._consumers instanceof Consumer) {
      this._consumers = [this._consumers, consumer];
    } else {
      this._consumers.push(consumer);
    }
    this._optimizePush();
  }
  delete(consumer: Consumer<VALUE>): boolean {
    const sizeBefore = this.size;

    if (this._consumers === consumer) {
      this._consumers = undefined;
    } else if (Array.isArray(this._consumers)) {
      this._consumers = this._consumers.filter((item) => item !== consumer);
      if (this._consumers.length === 1) this._consumers = this._consumers[0];
    }
    if (this.size < sizeBefore) {
      this._optimizePush();
      return true;
    }

    return false;
  }
  clear(): void {
    this._consumers = undefined;
  }
}
