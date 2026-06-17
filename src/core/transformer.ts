import { Smoker } from "./smoker";

export abstract class Transformer<INPUT extends Smoker.AnySmoker, VALUE, NAME extends string> extends Smoker<
  VALUE,
  NAME
> {
  constructor(
    name: NAME,
    protected readonly input: INPUT,
    init: Transformer.Init<VALUE, NAME>,
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

    let cleanup: () => void;
    super({
      ...init,
      name,
      scope,
      source: undefined, /// must be connected to a consumer
      onEvent: (e) => {
        switch (e.type) {
          case "consumer-join":
            if (this.get("consumersCount") === 1) cleanup = init.source();
            break;
          case "consumer-left":
            if (this.get("consumersCount") === 0) cleanup();
            break;
        }
        init.onEvent?.(e);
      },
    });

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
  export type Init<VALUE, NAME extends string> = Omit<Smoker.Init<VALUE, NAME>, "source"> & {
    source: () => () => void;
  };
  export type AnyTransformer = Transformer<Smoker.AnySmoker, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractSmoker<T> = T extends Transformer<infer INPUT, any, any> ? INPUT : never;
  export type Traversable<T extends Smoker.AnySmoker> =
    ExtractSmoker<T> extends never
      ? T
      : Omit<T, "traversal"> & Record<ExtractSmoker<T>["name"] | (`$${string}` & {}), Traversable<ExtractSmoker<T>>>;
}
