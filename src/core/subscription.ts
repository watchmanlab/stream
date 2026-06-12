import type { Vapor } from "./vapor";

export class Subscription<VALUE> implements Subscription.Executor<VALUE> {
  private isReady: boolean = true;
  private sharedQueueIndex: number = -1;
  private listener: Subscription.Listener<VALUE>;
  private subscriptionIndex: number;
  private sharedQueue: Vapor.SharedQueue<VALUE>;
  private onDrain: () => void;
  private onAbort: () => void;
  private isProcessing = false;
  value: VALUE = undefined as VALUE;
  constructor(init: Subscription.Init<VALUE>) {
    this.listener = init.listener;
    this.subscriptionIndex = init.subscriptionIndex;
    this.sharedQueue = init.sharedQueue;
    this.onDrain = init.onDrain;
    this.onAbort = init.onAbort;
  }

  executeHot(value: VALUE) {
    if (this.isProcessing) return;

    this.isProcessing = true;
    this.isReady = false;
    this.value = value;
    try {
      this.listener(this);
    } finally {
      this.isProcessing = false;
    }

    if (this.isReady) {
      this.drain();
    }
  }
  ready() {
    this.isReady = true;
    this.drain();
  }
  drain() {
    if (this.isProcessing) return;

    this.isProcessing = true;
    try {
      while (this.isReady && this.sharedQueueIndex !== -1 && this.sharedQueueIndex < this.sharedQueue.length) {
        this.isReady = false;

        const item = this.sharedQueue[this.sharedQueueIndex]!;
        this.value = item.value;

        item.pending--;

        this.onDrain();

        this.sharedQueueIndex++;
        if (this.sharedQueueIndex >= this.sharedQueue.length) {
          this.sharedQueueIndex = -1;
        }

        this.listener(this);
      }
    } finally {
      this.isProcessing = false;
    }
  }
  abort() {
    this.onAbort();
  }
}

export namespace Subscription {
  export type Abort = () => void;
  export type Ready = () => void;
  export interface Executor<VALUE> {
    value: VALUE;
    ready: Ready;
    abort: Abort;
  }
  export type Listener<VALUE> = (executor: Executor<VALUE>) => void;

  export type Init<VALUE> = {
    listener: Subscription.Listener<VALUE>;
    subscriptionIndex: number;
    sharedQueue: Vapor.SharedQueue<VALUE>;
    onDrain: () => void;
    onAbort: () => void;
  };
}
