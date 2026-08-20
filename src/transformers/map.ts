import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { DefaultQueue } from "../core/default-queue";
import { Queue } from "../core/queue";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";

import { ExtractValue, TerminateReason } from "../core/types";

export class Map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>
  extends Source<MAPPED>
  implements Transformer<INPUT, MAPPED>
{
  constructor(
    readonly $input: INPUT,
    private mapper: Map.Mapper<VALUE, MAPPED>,
  ) {
    super();
  }
  override consume(handler: Consumer.Handler<MAPPED>, options?: Consumer.Options<MAPPED>): Consumer<MAPPED> {
    return this.$input.consume(
      (consumer, value) => handler(consumer, this.mapper(value)),
      new ConsumerOptions(options),
    );
  }
}
//this will later be moved to  the consumer module
class DefaultOptions<T> implements Required<Consumer.Options<T>> {
  constructor(private options?: Consumer.Options<T>) {}
  get passive(): boolean {
    return false;
  }
  queueFactory(): Queue<T> {
    return this?.options?.queueFactory?.() ?? new DefaultQueue();
  }
  init(consumer: Consumer<T>): undefined | Consumer.InitCleanup {
    return this?.options?.init?.(consumer);
  }
  push(consumer: Consumer<T>, value: T): void {
    return this?.options?.push?.(consumer, value);
  }
  next(consumer: Consumer<T>): void {
    return this?.options?.next?.(consumer);
  }
  drain(consumer: Consumer<T>): void {
    return this?.options?.drain?.(consumer);
  }
  enqueue(consumer: Consumer<T>, value: T): void {
    return this?.options?.enqueue?.(consumer, value);
  }
  dequeue(consumer: Consumer<T>, value: T): void {
    return this?.options?.dequeue?.(consumer, value);
  }
  terminate(consumer: Consumer<T>, reason: TerminateReason): void {
    return this?.options?.terminate?.(consumer, reason);
  }
}
// as usage example
class ConsumerOptions<T> extends DefaultOptions<T> {
  constructor(options: Consumer.Options<T> = {}) {
    super(options);
  }
  override next(consumer: Consumer<T>) {
    new AbortController();
    super.next(consumer);
  }
  override terminate(consumer: Consumer<T>, reason: TerminateReason): void {
    new AbortController();
    super.terminate(consumer, reason);
  }
  override push(consumer: Consumer<T>, value: T): void {
    new AbortController();
    super.push(consumer, value);
  }
}

export function map<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  MAPPED = VALUE,
>(mapper: Map.Mapper<VALUE, MAPPED>) {
  return ($input: INPUT) => new Map($input, mapper);
}

export namespace Map {
  export type Mapper<VALUE, MAPPED> = (value: VALUE) => MAPPED;
}
