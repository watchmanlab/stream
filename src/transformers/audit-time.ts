import { InfosLinker } from "../core/infos-linker";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class AuditTime<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "auditTime",
> extends Transformer<INPUT, VALUE, NAME> {
  protected override _infosLinker: InfosLinker<AuditTime.Infos>;
  constructor(input: INPUT, ms: number, options?: AuditTime.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("auditTime" as NAME),
      source: {
        listen: (handler, options) => {
          let latest: VALUE;
          let timer: any = null;

          return input.listen(
            (self, value) => {
              latest = value;
              if (!timer) {
                timer = setTimeout(() => {
                  handler(self, latest);
                  timer = null;
                }, ms);
              }
            },
            {
              ...options,
              events: {
                ...options?.events,
                abort(self, value) {
                  clearTimeout(timer);
                  options?.events?.abort?.(self, value);
                },
                complete(self, value) {
                  clearTimeout(timer);
                  options?.events?.complete?.(self, value);
                },
              },
            },
          );
        },
      },
    });

    this._infosLinker = new InfosLinker({
      ms: () => ms,
      state: () => this._state,
      consumersCount: () => this._consumers.size,
    });
  }

  override get infos(): AuditTime.Infos {
    return this._infosLinker.infos;
  }
}

export function auditTime<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "auditTime",
>(ms: number, options?: AuditTime.Options<VALUE, NAME>): Transform<INPUT, NAME, AuditTime<INPUT, VALUE, NAME>> {
  return (input, name) => new AuditTime(input, ms, { ...options, name: name ?? options?.name });
}

export namespace AuditTime {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
  export type Infos = Stream.Infos & { ms: number };
}
