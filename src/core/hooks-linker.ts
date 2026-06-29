export class HooksLinker<TRAPPED extends Record<string, (...args: any) => any>, CONTEXT> {
  constructor(
    private context: CONTEXT,
    private hooks?: HooksLinker.Hooks<TRAPPED, CONTEXT>,
  ) {}
  hook<
    KEY extends keyof TRAPPED,
    FN extends TRAPPED[KEY],
    ARGS extends Parameters<FN>,
    RETURN extends ReturnType<TRAPPED[KEY]>,
  >(hookName: KEY, trapped: FN): (...args: ARGS) => RETURN {
    const { hooks } = this;
    return hooks?.[hookName] ? (...args: ARGS) => hooks[hookName]!(this.context, trapped, ...args) : trapped;
  }
}

export namespace HooksLinker {
  export type Hooks<TRAPPED extends Record<string, (...args: any) => any>, CONTEXT> = {
    [K in keyof TRAPPED]?: (
      context: CONTEXT,
      trapped: TRAPPED[K],
      ...args: Parameters<TRAPPED[K]>
    ) => ReturnType<TRAPPED[K]>;
  };
}
