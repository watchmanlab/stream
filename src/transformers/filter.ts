import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";

import { ExtractValue, Transformer, AnyConsumable, Consumable } from "../core/types";

export class Filter<
  INPUT extends AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
>
  extends Source<FILTERED>
  implements Transformer<INPUT, FILTERED>
{
  constructor(
    readonly $input: INPUT,
    private predicate: Filter.Predicate<VALUE, FILTERED>,
  ) {
    super();
  }
  private _$others?: Stream<VALUE>;

  get $others(): Source<VALUE> {
    return (this._$others ??= new Stream({
      lastConsumerLeft: () => (this._$others = undefined),
    })).asSource();
  }
  consume(handler: Consumer.Handler<FILTERED>, options?: Consumer.Options<FILTERED>): Consumer<FILTERED> {
    return this.$input.consume((consumer, value) => {
      if (this.predicate(value)) {
        handler(consumer, value);
      } else {
        this._$others?.push(value);
        consumer.next();
      }
    }, options);
  }
}

export function filter<
  INPUT extends AnyConsumable,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  FILTERED extends VALUE = VALUE,
>(predicate: Filter.Predicate<VALUE, FILTERED>) {
  return ($input: INPUT) => new Filter($input, predicate);
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE) => value is FILTERED)
    | ((value: VALUE) => boolean);
}
