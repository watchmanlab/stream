import { Stream } from "../../../streams/index.ts";
import { effect } from "../effect/effect.ts";
import { Map } from "../../map.ts";
import { pump } from "../pump/pump.ts";

const NAME = "pluck";

export class Pluck<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  KEY extends keyof CLEAN_VALUE = keyof CLEAN_VALUE,
  ERROR = never,
  NAME extends string = pluck.Name,
> extends Map<SOURCE, CLEAN_VALUE, CLEAN_VALUE[KEY], ERROR, NAME> {
  constructor(source: SOURCE, name = NAME as NAME, key: KEY) {
    super(source, name, (value) => value[key]);
  }
}

export function pluck<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  KEY extends keyof CLEAN_VALUE = keyof CLEAN_VALUE,
  ERROR = never,
  NAME extends string = pluck.Name,
>(key: KEY): Stream.Transform<NAME, SOURCE, Pluck<SOURCE, CLEAN_VALUE, KEY, ERROR, NAME>> {
  return (_, source, name) => new Pluck(source, name, key);
}

export namespace pluck {
  export type Name = typeof NAME;
}
