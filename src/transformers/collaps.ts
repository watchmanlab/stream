import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";

export class Collaps<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  NAME extends string = collaps.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = collaps.NAME as NAME, input: INPUT) {
    // Walk back through input chain, find all collaps
    const operatorChain = Collaps._extractOperatorChain(input);

    if (operatorChain.length === 0) {
      // No collaps to collapse, just pass through
      super(name, input, {
        source: {
          listen: (init) => input.listen(init),
        },
      });
      return;
    }

    // Compile collaps into single function
    const compiledFn = Collaps._compile(operatorChain);

    // Create transformer that applies compiled function
    super(name, input, {
      source: {
        listen: (init) => {
          // Listen to the ROOT stream (before collaps)
          const rootStream = operatorChain[0].input;

          return rootStream.listen({
            ...init,
            handler: (self, value) => {
              const result = compiledFn(value);
              if (result !== undefined) {
                init.handler(self, result);
              } else {
                self.next(); // Filtered out
              }
            },
          });
        },
      },
    });
  }

  private static _extractOperatorChain(stream: Stream.AnyStream): Array<{
    transformer: Transformer<any, any, any>;
    operator: Transformer.Operator<any, any>;
    input: Stream.AnyStream;
  }> {
    const chain: Array<any> = [];
    let current = stream;

    // Walk backwards through transformers
    while (current instanceof Transformer) {
      const operator = current.asOperator?.();

      if (!operator) {
        // Hit non-operator transformer, stop
        break;
      }

      chain.unshift({
        transformer: current,
        operator,
        input: current.traversal.$input,
      });

      current = current.traversal.$input;
    }

    return chain;
  }

  private static _compile(chain: Array<{ operator: Transformer.Operator<any, any> }>): (value: any) => any {
    const collaps = chain.map((c) => c.operator);

    return (value: any) => {
      let current = value;
      for (const op of collaps) {
        current = op.execute(current);
        if (current === undefined) {
          return undefined; // Filtered
        }
      }
      return current;
    };
  }
}

export function collaps<
  INPUT extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT> = Stream.ExtractValue<INPUT>,
  NAME extends string = collaps.Name,
>(): Stream.Transform<INPUT, NAME, Collaps<INPUT, VALUE, NAME>> {
  return (input, name) => new Collaps(name, input);
}

export namespace collaps {
  export const NAME = "collaps";
  export type Name = typeof NAME;
}
