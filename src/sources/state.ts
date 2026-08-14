import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { NonEmptyString, Source } from "../core/types";

export class State<VALUE> implements Source<VALUE> {
  private _value: VALUE;
  private _$stream?: Stream<VALUE, "$state">;

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
    return (this._$stream ??= new Stream({
      name: "$state",
      lastConsumerLeft: (stream, consumer) => {
        stream.terminate("complete");
        this._$stream = undefined;
      },
    })).consume(handler, options);
  }
}
