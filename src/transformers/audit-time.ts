import { Stream, Transformer } from "../core/index.ts";

const NAME = "auditTime";

export class AuditTime<INPUT_STREAM extends Stream.AnyStream, NAME extends string = auditTime.Name> extends Transformer<
  INPUT_STREAM,
  Stream.ExtractValue<INPUT_STREAM>,
  NAME
> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, ms: number) {
    let timer: any = null;
    let canEmit = true;
    super(
      name,
      inputStream,
      inputStream.consumers.get({
        next: (batch) => {
          batch.forEach((value) => {
            if (!canEmit) return;
            canEmit = false;
            clearTimeout(timer);
            timer = setTimeout(() => (canEmit = true), ms);
            this.push(value);
          });
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}

export function auditTime<INPUT_STREAM extends Stream.AnyStream, NAME extends string = auditTime.Name>(
  ms: number,
): Stream.Transform<INPUT_STREAM, NAME, AuditTime<INPUT_STREAM, NAME>> {
  return (inputSteam, name) => new AuditTime(name, inputSteam, ms);
}

export namespace auditTime {
  export type Name = typeof NAME;
}
