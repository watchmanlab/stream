import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class AuditTime<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "auditTime",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    input: INPUT,
    public readonly ms: number,
    options?: AuditTime.Options<VALUE, NAME>,
  ) {
    super(input, {
      ...options,
      name: options?.name ?? ("auditTime" as NAME),
      source: {
        listen: (handler, options) => {
          let latest: VALUE;
          let timer: any = null;

          return input.listen(
            (context, value) => {
              latest = value;
              if (!timer) {
                timer = setTimeout(() => {
                  handler(context, latest);
                  timer = null;
                }, ms);
              }
            },
            {
              ...options,
              events: {
                ...options?.events,
                abort(context, value) {
                  clearTimeout(timer);
                  options?.events?.abort?.(context, value);
                },
                complete(context, value) {
                  clearTimeout(timer);
                  options?.events?.complete?.(context, value);
                },
              },
            },
          );
        },
      },
    });
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
}
