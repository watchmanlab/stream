import { Stream } from "../../stream/stream";

const NAME = "consumer";
export class Consumer<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
> implements Disposable {
  private _fn: Consumer.Fn<VALUE, this>;
  private _buffer: VALUE[];
  private _isReady: boolean;

  constructor(
    inputStream: INPUT_STREAM,
    fn: Consumer.Fn<VALUE, Consumer<INPUT_STREAM, VALUE>>,
    options: { isReady: boolean } = { isReady: true },
  ) {
    this._fn = fn;
    this._buffer = [];
    this._isReady = options.isReady;

    inputStream.listen((value) => {
      if (this._isReady) {
        this._isReady = false;
        this._fn(value, this);
      } else {
        this._buffer.push(value);
      }
    });
  }

  get isReady() {
    return this._isReady;
  }
  get buffer() {
    return this._buffer;
  }
  get fn() {
    return this._fn;
  }
  private calls = 0;
  ready = () => {
    const value = this._buffer.shift();
    if (!value) {
      this._isReady = true;
      return;
    }

    this._isReady = false;

    if (this.calls > 100) {
      this.calls = 0;
      queueMicrotask(() => {
        this._fn(value, this);
      });
    } else {
      this.calls++;
      this._fn(value, this);
    }
  };
  abort = () => {};
  [Symbol.dispose]() {
    this.abort();
  }
}

export namespace Consumer {
  export type Name = typeof NAME;
  export type Fn<VALUE, SELF extends Consumer<any, any>> = (value: VALUE, self: SELF) => void;
}

function simpleTest() {
  const stream = new Stream<number>();

  const { ready } = new Consumer(stream, async (value, { ready, buffer }) => {
    ready();
    await new Promise((r) => setTimeout(r, Math.random() * 200));
    console.log(value);
  });

  stream.push(1);
  stream.push(2);
  stream.push(3);
}
function bench() {
  const MAX = 10_000_000;
  const start = performance.now();
  const stream = new Stream<number>();

  const { ready } = new Consumer(
    stream,
    (value, { ready, buffer }) => {
      // await new Promise((r) => setTimeout(r));
      if (value === MAX) console.log("consumer", value.toExponential(), Math.round(performance.now() - start));
      ready();
    },
    { isReady: false },
  );

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
  ready();
}
// simpleTest();
bench();
