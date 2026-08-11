import { Collection } from "./types";

export class ArrayCollection<T> implements Collection<T> {
  private _size = 0;
  get size() {
    return this._size;
  }
  add(item: T): void {}
  delete(item: T): void {}
  clear(): void {}
}
