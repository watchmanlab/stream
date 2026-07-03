import { InfosLinker } from "../core/infos-linker";
import { LinkedList } from "../core/linked-list";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, FixedArray, NonEmptyString, Queue, Transform } from "../core/types";

export class BufferCount<
  INPUT extends AnyStream,
  SIZE extends number,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "bufferCount",
> extends Transformer<INPUT, FixedArray<VALUE, SIZE>, NAME> {
  private _buffers = new LinkedList<VALUE[]>();
  protected override _infosLinker: InfosLinker<BufferCount.Infos<VALUE>>;
  constructor(
    input: INPUT,
    public readonly size: SIZE,
    public readonly startBufferEvery = size,
    options?: BufferCount.Options<FixedArray<VALUE, SIZE>, NAME>,
  ) {
    let count = 0;

    super(input, {
      ...options,
      name: options?.name ?? ("bufferCount" as NAME),
      source: {
        listen: (handler, options) => {
          return input.listen(
            (self, value) => {
              if (count % startBufferEvery === 0) this._buffers.enqueue([]);

              let buffersSize = this._buffers.size;
              for (const buffer of this._buffers) {
                buffer.push(value);
                if (buffer.length === size) {
                  this._buffers.dequeue();
                  handler(self, [...buffer] as FixedArray<VALUE, SIZE>);
                }
              }

              if (buffersSize === this._buffers.size) self.next();

              count++;
            },
            {
              ...options,
              events: {
                ...options?.events,
                abort: (context, value) => {
                  this._buffers.clear();
                  count = 0;
                  options?.events?.abort?.(context, value);
                },

                complete: (context, value) => {
                  while (this._buffers.size > 0) {
                    const buffer = this._buffers.dequeue() as VALUE[];

                    if (buffer.length > 0) {
                      handler(context, [...buffer] as never);
                    }
                  }
                  count = 0;
                  options?.events?.complete?.(context, value);
                },
              },
            },
          );
        },
      },
    });

    this._infosLinker = new InfosLinker({
      buffers: () => [...this._buffers].map((buffer) => buffer.values()),
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
  export type Infos<VALUE> = Stream.Infos & { buffers: ArrayIterator<VALUE>[] };
}
