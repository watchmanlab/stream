import { Stream } from "./stream";

export abstract class Transformer<SOURCE extends Stream<any, any>, VALUE, NAME extends string> extends Stream<
  VALUE,
  NAME
> {
  constructor(
    protected source: SOURCE,
    name: NAME,
    impl: Stream.Source<VALUE>,
  ) {
    super(name, impl);
    if (source.name in this) {
      throw new Error(
        `Naming conflict: Cannot name ${source.constructor.name} transformer with "${source.name}" ` +
          `because ${this.constructor.name} already has a property with that name.\n` +
          `Solutions:\n` +
          `  1. Use different name in pipe: .pipe("$${source.name}", ${source.constructor.name})\n` +
          `  2. Rename on stream creation: new Stream<T, "$${source.name}">()\n` +
          `conflictingProperty:${(this as any)[source.name]};`,
      );
    }
    (this as any)[source.name] = this;

    return new Proxy(this, {
      get(target: any, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return target["root"] || source;
      },
    });
  }
}

export namespace Transformer {
  export type Options<NAME extends string> = {
    name?: NAME;
  };
}
