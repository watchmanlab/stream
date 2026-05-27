import { Mitto } from "./src/mitto";
const m1 = new Mitto<number>();

m1.map((v) => {
  console.log("map", v);
  return v.toLocaleString();
})
  .filter((v) => {
    console.log("filrer", v);
    return v.length > 0;
  })
  .listen();

m1.emit(1);
m1.emit(2);
