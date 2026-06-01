import { Mitto } from "./src/mitto";
const m1 = new Mitto({
  source: function* () {
    yield 1;
    yield 2;
    yield 3;
  },
});

let s = m1.listen((v) => {
  console.log("l1", v);
  s.emit();
});

setTimeout(() => {
  s = m1.listen((v) => {
    console.log("l2", v);
    s.emit();
  });
}, 1000);
setTimeout(() => {
  s = m1.listen((v) => {
    console.log("l3", v);
    s.emit();
  });
}, 2000);
