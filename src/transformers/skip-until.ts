import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Transformer } from "../core/transformer";
import { ExtractValue, TerminateReason } from "../core/types";

export class SkipUntil<INPUT extends Consumable.AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private $notifier: Consumable.AnyConsumable,
  ) {
    super();
  }
  consume(options?: Consumer.Options<VALUE>): Consumer<VALUE> {
    return this.$input.consume();
  }
}

export function skipUntil<INPUT extends Consumable.AnyConsumable>($notifier: Consumable.AnyConsumable) {
  return ($input: INPUT) => new SkipUntil($input, $notifier);
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  private skipping = true;

  constructor(
    private predicate: (value: any) => boolean,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }

  override handler(consumer: Consumer<any>, value: any): void {}
}
class InputConsumerOptions extends Consumer.DefaultOptions<any> {
  private notifierConsumer!: Consumer<any>;
  skipping = true;
  constructor(
    private $notifier: Consumable<any>,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override init(consumer: Consumer<any>): void | undefined {
    super.init(consumer);
    this.notifierConsumer = this.$notifier.consume(new NotifierConsumerOptions(consumer));
  }
  override handler(consumer: Consumer<any>, value: any): void {
    if (this.skipping) {
      consumer.next();
      return;
    }

    this.options?.handler?.(consumer, value);
  }
  override terminate(consumer: Consumer<any>, reason: TerminateReason): void {
    this.notifierConsumer.terminate(reason);
    super.terminate(consumer, reason);
  }
}
class NotifierConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(private inuputConsumer: Consumer<any>) {
    super();
  }
  override handler(consumer: Consumer<any>, value: any): void {
    consumer.terminate("complete");

    this.inuputConsumer.terminate("complete");
  }
  override terminate(consumer: Consumer<any>, reason: TerminateReason): void {
    this.inuputConsumer.terminate(reason);
    super.terminate(consumer, reason);
  }
}
