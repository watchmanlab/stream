export class HooksLinker<HOOKS extends Record<string, (...args: any[]) => void>, TARGET> {
  constructor(
    private targer: TARGET,
    private hooks: HOOKS,
  ) {}
}

export namespace HooksLinker {}
