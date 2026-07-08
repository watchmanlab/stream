// for now it's not been used for immutability reason

export class HooksLinker<TRAPPED extends Record<string, (...args: any) => any>, SELF> {
  constructor(
    private self: SELF,
    private hooks?: HooksLinker.Hooks<TRAPPED, SELF>,
  ) {}
  hook<
    KEY extends keyof TRAPPED,
    FN extends TRAPPED[KEY],
    ARGS extends Parameters<FN>,
    RETURN extends ReturnType<TRAPPED[KEY]>,
  >(hookName: KEY, trapped: FN): (...args: ARGS) => RETURN {
    const { hooks } = this;
    return hooks?.[hookName] ? (...args: ARGS) => hooks[hookName]!(this.self, trapped, ...args) : trapped;
  }
}

export namespace HooksLinker {
  export type Hooks<TRAPPED extends Record<string, (...args: any) => any>, SELF> = {
    [K in keyof TRAPPED]?: (self: SELF, trapped: TRAPPED[K], ...args: Parameters<TRAPPED[K]>) => ReturnType<TRAPPED[K]>;
  };
}
