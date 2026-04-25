import { Stream } from "./stream";

const NAME = "consumer";
export class Consumer<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = consumer.Name,
> extends Stream<VALUE, NAME> {
  readonly transformer: Stream.Transformer<this, INPUT_STREAM>;
  private _fn: consumer.Fn<VALUE, Stream.Transformer<this, INPUT_STREAM>>;
  private _buffer: VALUE[];
  private _isReady: boolean;
  private _options: typeof consumer.defaultOptions;

  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    fn: consumer.Fn<VALUE, Stream.Transformer<Consumer<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>,
    options?: consumer.Options,
  ) {
    super(name);
    this._options = { ...consumer.defaultOptions, ...options };

    this.transformer = Stream.transformer(this, inputStream);
    this._fn = fn;
    this._buffer = [];
    this._isReady = this._options.isReady;

    inputStream.listen((value) => {
      if (this._isReady) {
        this._isReady = false;
        this._fn(value, this.transformer);
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
  ready = () => {
    if (this._isReady) return;
    this._isReady = true;
    const value = this._buffer.shift();
    if (!value) return;

    this._isReady = false;
    this._fn(value, this.transformer);
  };
  abort = () => {};
  override [Symbol.dispose]() {
    super[Symbol.dispose]();
    this.abort();
  }
}

export function consumer<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = consumer.Name,
>(
  fn: consumer.Fn<VALUE, Stream.Transformer<Consumer<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>,
  options?: consumer.Options,
): Stream.Transform<INPUT_STREAM, NAME, Stream.Transformer<Consumer<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream, name) => new Consumer(name, inputStream, fn, options).transformer;
}

export namespace consumer {
  export type Name = typeof NAME;
  export type Fn<VALUE, SELF extends Consumer<any, any, any>> = (value: VALUE, consumer: SELF) => void;
  export type Options = { isReady?: boolean };
  export const defaultOptions: Required<Options> = { isReady: true };
}

function simpleTest() {
  const stream = new Stream<number>();

  const { ready } = stream.pipe(
    consumer(async (value, { ready, buffer }) => {
      // ready();
      await new Promise((r) => setTimeout(r, Math.random() * 200));
      console.log(value, buffer);
    }),
  );

  stream.push(1);
  stream.push(2);
  stream.push(3);
}

simpleTest();
