import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { ExtractValue } from "../core/types";

export class Filter<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
>
  extends Source<FILTERED>
  implements Transformer<INPUT, FILTERED>
{
  private _$complement?: Stream<VALUE>;
  constructor(
    readonly $input: INPUT,
    private predicate: Filter.Predicate<VALUE, FILTERED>,
    private complement?: (value: VALUE) => void,
  ) {
    super();
  }

  get $complement(): Source<VALUE> {
    return Source.from(
      (this._$complement ??= new Stream({
        lastConsumerLeft: () => (this._$complement = undefined),
      })),
    );
  }
  consume(options?: Consumer.Options<FILTERED>): Consumer<FILTERED> {
    return this.$input.consume(new ConsumerOptions(this, options));
  }
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private filter: Filter<any>,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override handler(consumer: Consumer<any>, value: any): void | undefined {
    if (this.filter["predicate"](value)) {
      super.handler(consumer, value);
    } else {
      this.filter["_$complement"]?.push(value);
      this.filter["complement"]?.(value);
      consumer.next();
    }
  }
}

export function filter<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
>(predicate: Filter.Predicate<VALUE, FILTERED>, complement?: (value: VALUE) => void) {
  return ($input: INPUT) => new Filter($input, predicate, complement);
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);
}
