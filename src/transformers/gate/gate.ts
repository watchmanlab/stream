import { Stream } from "../../streams/index.ts";

const NAME = "gate";

export class Gate<
  SOURCE extends Stream<any, any>,
  VALUE extends Stream.ExtractValue<SOURCE> = Stream.ExtractValue<SOURCE>,
  NAME extends string = gate.Name,
> extends Stream<VALUE, NAME> {
  protected _isOpen = true;
  protected _resolver?: () => void;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void, any> | undefined;
  constructor(source: SOURCE, name = NAME as NAME, isOpen = true) {
    super(name, async function* () {
      try {
        while (true) {
          if (!self._isOpen) await new Promise<void>((r) => (self._resolver = r));
          if (!self.__sourceGenerator) self.__sourceGenerator = source[Symbol.asyncIterator]();

          let result = await self.__sourceGenerator.next();

          while (!result.done && self._isOpen) {
            yield result.value;
            result = await self.__sourceGenerator.next();
          }

          if (self.__sourceGenerator) break;
        }
      } finally {
        self._resolver?.();
        self.__sourceGenerator?.return();
      }
    });
    const self = this;
    this._isOpen = isOpen;
  }

  open() {
    this._isOpen = true;
    this._resolver?.();
  }
  close() {
    this._isOpen = false;
    this.__sourceGenerator?.return();
  }
}

export function gate<
  SOURCE extends Stream<any, any>,
  VALUE extends Stream.ExtractValue<SOURCE> = Stream.ExtractValue<SOURCE>,
  NAME extends string = gate.Name,
>(isOpen = true): Stream.Transforme<NAME, SOURCE, Gate<SOURCE, VALUE, NAME>> {
  return (_, source, name) => new Gate(source, name, isOpen);
}

export namespace gate {
  export type Name = typeof NAME;
  export type Gate = {
    open(): void;
    close(): void;
    readonly isOpen: boolean;
  };
}
