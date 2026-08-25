import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { ExtractValue } from "../core/types";

export class Filter<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
> extends Source<FILTERED> {
  constructor(
    readonly $input: INPUT,
    private predicate: Filter.Predicate<VALUE, FILTERED>,
    private complement?: (value: VALUE) => void,
  ) {
    super();
  }
  private _$complements?: Stream<VALUE>;

  get $complements(): Source<VALUE> {
    return Source.from(
      (this._$complements ??= new Stream({
        lastConsumerLeft: () => (this._$complements = undefined),
      })),
    );
  }
  consume(handler: Consumer.Handler<FILTERED>, options?: Consumer.Options<FILTERED>): Consumer<FILTERED> {
    let index = 0;
    return this.$input.consume((consumer, value) => {
      if (this.predicate(value, index++)) {
        handler(consumer, value);
      } else {
        this._$complements?.push(value);
        this.complement?.(value);
        consumer.next();
      }
    }, options);
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
    | ((value: VALUE, index: number) => value is FILTERED)
    | ((value: VALUE, index: number) => boolean);
}
