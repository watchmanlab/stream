import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";

export class State<VALUE> extends Source<VALUE> {
  private _value: VALUE;
  private _stream?: Stream<VALUE>;

  constructor(initialValue: VALUE) {
    super();
    this._value = initialValue;
  }
  get value(): VALUE {
    return this._value;
  }
  set value(v: VALUE) {
    this._value = v;
    this._stream?.push(v);
  }

  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE> | undefined): Consumer<VALUE> {
    return (this._stream ??= new Stream({
      lastConsumerLeft: (stream, consumer) => {
        stream.terminate("complete");
        this._stream = undefined;
      },
    })).consume(handler, options);
  }
}
