import { EMPTY_FUNCTION } from "./consts";
import { Consumer } from "./consumer";
import { ConsumerSet, TerminateReason } from "./types";

export class DefaultConsumerSet<VALUE> implements ConsumerSet<VALUE> {
  private _consumers?: Set<Consumer<VALUE>> | Consumer<VALUE>;

  get size(): number {
    return this._consumers instanceof Consumer ? 1 : (this._consumers?.size ?? 0);
  }

  private _optimizePush(): void {
    if (this._consumers instanceof Set) {
      const snapshot = Array.from(this._consumers);
      const length = snapshot.length;
      this.push = (value) => {
        for (let i = 0; i < length; i++) {
          snapshot[i].push(value);
        }
      };
    } else if (this._consumers) {
      const consumer = this._consumers;
      this.push = (value) => {
        consumer.push(value);
      };
    } else {
      this.push = EMPTY_FUNCTION;
    }
  }
  push(value: VALUE) {}

  add(consumer: Consumer<VALUE>): ConsumerSet.Delete {
    if (!this._consumers) {
      this._consumers = consumer;
    } else if (this._consumers instanceof Consumer) {
      this._consumers = new Set([this._consumers, consumer]);
    } else {
      this._consumers.add(consumer);
    }
    this._optimizePush();

    return () => this._delete(consumer);
  }
  private _delete(consumer: Consumer<VALUE>) {
    const sizeBefore = this.size;
    if (!this._consumers) {
      return false;
    } else if (this._consumers === consumer) {
      this._consumers = undefined;
    } else if (this._consumers instanceof Set) {
      this._consumers.delete(consumer);
      if (this._consumers.size === 1) this._consumers = this._consumers.values().next().value!;
    }
    if (this.size < sizeBefore) {
      this._optimizePush();
      return true;
    }
    return false;
  }
  terminate(reason: TerminateReason): void {
    if (this._consumers instanceof Set) {
      const snapshot = Array.from(this._consumers);
      for (let i = 0; i < snapshot.length; i++) {
        const consumer = snapshot[i];
        if (this._consumers.has(consumer)) {
          consumer.terminate(reason);
        }
      }
    } else {
      this._consumers?.terminate(reason);
    }
  }
}
