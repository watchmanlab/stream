import { InfosLinker } from "../core/infos-linker";
import { LinkedListQueue } from "../core/linked-list-queue";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, FixedArray, NonEmptyString, Queue, Transform } from "../core/types";

export class BufferCount<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "bufferCount",
> extends Transformer<INPUT, FixedArray<VALUE, SIZE>, NAME> {
  protected override _infosLinker: InfosLinker<BufferCount.Infos<VALUE>>;
  constructor(
    input: INPUT,
    size: SIZE,
    startBufferEvery = size,
    options?: BufferCount.Options<FixedArray<VALUE, SIZE>, NAME>,
  ) {
    const buffers = new LinkedListQueue<VALUE[]>();
    let count = 0;

    super(input, {
      ...options,
      name: options?.name ?? ("bufferCount" as NAME),
      source: {
        consume: (handler, options) => {
          return input.consume(
            (self, value) => {
              if (count++ % startBufferEvery === 0) buffers.enqueue([]);

              let buffersSize = buffers.size;
              //since there is no reentrancy issue because of a safe queue mutation inside a loop ,
              // we always have a single buffer full at most at a time
              for (const buffer of buffers) {
                buffer.push(value);
                if (buffer.length === size) {
                  buffers.dequeue(); //safe
                  handler(self, [...buffer] as FixedArray<VALUE, SIZE>);
                }
              }

              if (buffersSize === buffers.size) self.next();
            },
            {
              ...options,
              events: {
                ...options?.events,
                abort: (self, value) => {
                  buffers.clear();
                  count = 0;
                  options?.events?.abort?.(self, value);
                },

                complete: (self, value) => {
                  while (buffers.size > 0) {
                    const buffer = buffers.dequeue() as VALUE[];

                    if (buffer.length > 0) {
                      handler(self, [...buffer] as never);
                    }
                  }
                  count = 0;
                  options?.events?.complete?.(self, value);
                },
              },
            },
          );
        },
      },
    });

    this._infosLinker = new InfosLinker({
      size: () => size,
      startBufferEvery: () => startBufferEvery,
      buffers: () => [...buffers].map((buffer) => buffer.values()),
      consumersCount: () => this._consumers.size,
      state: () => this._state,
    });
  }

  override get infos(): BufferCount.Infos<VALUE> {
    return this._infosLinker.infos;
  }
}

export function bufferCount<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "bufferCount",
>(
  size: SIZE,
  startBufferEvery = size,
  options?: BufferCount.Options<FixedArray<VALUE, SIZE>, NAME>,
): Transform<INPUT, NAME, BufferCount<INPUT, SIZE, VALUE, NAME>> {
  return (input, name) => new BufferCount(input, size, startBufferEvery, { ...options, name: name ?? options?.name });
}
export namespace BufferCount {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
  export type Infos<VALUE> = Stream.Infos & { buffers: ArrayIterator<VALUE>[]; size: number; startBufferEvery: number };
}
