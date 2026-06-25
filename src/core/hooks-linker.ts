export class HooksLinker<FNS extends Record<string, any[]>, SELF> {
  constructor(
    private self: SELF,
    private hooks?: HooksLinker.Hooks<FNS, SELF>,
  ) {}
  hook<KEY extends keyof FNS, ARGS extends FNS[KEY]>(hookName: KEY, trapped: () => void, ...args: ARGS): void {
    const { hooks } = this;
    return hooks?.[hookName] ? hooks[hookName](this.self, trapped, ...args) : trapped();
  }
}

export namespace HooksLinker {
  export type Hooks<FNS extends Record<string, any[]>, SELF> = {
    [K in keyof FNS]?: (self: SELF, next: () => void, ...args: FNS[K]) => void;
  };

  export type TrappedFromEvents<
    EVENTS extends Record<string, unknown>,
    FNS extends { [K in keyof EVENTS]: [EVENTS[K]] } = {
      [K in keyof EVENTS]: [EVENTS[K]];
    },
  > = FNS;
}
