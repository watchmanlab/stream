import { Consumer } from "../core/consumer";
import { Producer } from "../core/stream";
import { Consumable } from "../core/types";

export class State<VALUE> implements Consumable<VALUE> {
  private _value: VALUE;
  private _$stream?: Producer<VALUE>;

  constructor(initialValue: VALUE) {
    this._value = initialValue;
  }
  get value(): VALUE {
    return this._value;
  }
  set value(v: VALUE) {
    this._value = v;
    this._$stream?.push(v);
  }

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return (this._$stream ??= new Producer({
      lastConsumerLeft: (stream, consumer) => {
        stream.terminate("complete");
        this._$stream = undefined;
      },
    })).consume(handler, options);
  }
}
