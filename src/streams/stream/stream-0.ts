const NAME = "root";
export class Stream<VALUE = any, NAME extends string = Stream.Name> {
  private consumers: Stream.Consumer<VALUE>[] = [];
  private name: NAME;
  onConsumersAvailable: () => void = () => {};
  onConsumersGone: () => void = () => {};
  constructor();
  constructor(name: NAME);

  constructor(name = NAME as NAME) {
    this.name = name;
  }

  pipe<OUTPUT extends Stream<any, any>>(transformer: (source: this) => OUTPUT) {
    return transformer(this);
  }

  next(fn: Stream.Consumer<VALUE>) {
    const index =
      this.consumers.push((value) => {
        //
      }) - 1;
  }

  static push<VALUE>(stream: Stream<VALUE, any>, value: VALUE) {
    const consumers = stream.consumers;
    const len = consumers.length;
    for (let j = 0; j < len; j++) {
      consumers[j](value);
    }
  }
  static addConsumer<VALUE>(stream: Stream<VALUE, any>, consumer: Stream.Consumer<VALUE>) {
    stream.consumers.push(consumer);
    if (stream.consumers.length === 1) stream.onConsumersAvailable();
  }
  static removeConsumer<VALUE>(stream: Stream<VALUE, any>, consumer: Stream.Consumer<VALUE>) {
    const idx = stream.consumers.indexOf(consumer);
    if (idx !== -1) stream.consumers.splice(idx, 1);
    if (stream.consumers.length === 0) stream.onConsumersGone();
  }
}

export namespace Stream {
  export type Name = typeof NAME;
  export type Consumer<VALUE> = (value: VALUE) => void | Promise<void>;
}

/////////////////// SIMPLE BENCH ?????????????????
const stream = new Stream<number>();
const map =
  <T, U>(fn: (value: T) => U) =>
  (source: Stream<T>) => {
    const output = new Stream<U>();
    const consumer = (value: T) => Stream.push(output, fn(value));
    output.onConsumersAvailable = () => Stream.addConsumer(source, consumer);
    output.onConsumersGone = () => Stream.removeConsumer(source, consumer);

    return output;
  };

const mapped = stream
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3))
  .pipe(map((v) => v * 7))
  .pipe(map((v) => v / 9))
  .pipe(map((v) => v++))
  .pipe(map((v) => v - 3));

let count = 0;
let value = 0;
new Array(10).fill("").forEach(() => {
  Stream.addConsumer(mapped, (v) => {
    count++;
    value = v;
    if (v === -38) {
      v + 5;
    }
    if (v === -22) {
      v + 5;
    }
    if (v === -11) {
      v + 5;
    }
  });
});

const MAX = 1_00_000;

let i = 0;

const now = performance.now();

while (i <= MAX) {
  Stream.push(stream, i++);
}

console.log(performance.now() - now, count, value.toFixed());
