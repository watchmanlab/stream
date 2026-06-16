import { Smoker } from "./smoker";

export abstract class Transformer<INPUT extends Smoker.AnySmoker, VALUE, NAME extends string> extends Smoker<
  VALUE,
  NAME
> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    init: Smoker.Init<VALUE, NAME>,
  ) {
    let scope: Smoker.Scope | undefined = init.scope;

    if (scope instanceof Smoker) {
      scope = { any: [input, scope] };
    } else if (scope) {
      if (scope.any) {
        scope = { any: [input, ...scope.any] };
      } else {
        scope = { all: [input, ...scope.all] };
      }
    } else {
      scope = input;
    }

    super({ ...init, name, scope });

    this.input = input;

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return input;
      },
    });
  }
  get traversal(): Record<INPUT["name"] | (`$${string}` & {}), Transformer.Traversable<INPUT>> {
    const self = this;
    return new Proxy(
      {},
      {
        get() {
          return self.input;
        },
      },
    ) as never;
  }
}

export namespace Transformer {
  export type AnyTransformer = Transformer<Smoker.AnySmoker, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractSmoker<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
  export type Traversable<T extends Smoker.AnySmoker> =
    ExtractSmoker<T> extends never
      ? T
      : Omit<T, "traversal"> & Record<ExtractSmoker<T>["name"] | (`$${string}` & {}), Traversable<ExtractSmoker<T>>>;
}
