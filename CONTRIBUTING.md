# Contributing to @watchmanlab/stream

Thank you for helping scale the Pull-on-Push ecosystem! Our architecture is built on a single core principle: **Primitives must stay dumb; behavior lives in higher layers.**

We maintain a lightweight, flexible contribution process. No heavy framework rules, no overwhelming bureaucracy. Just clean, performant code.

---

## 🛠️ The Architecture Guideline

Every addition to the ecosystem should align with our three foundational primitives:

1. **Consumable** – Anything that can be listened to via `.consume()`.
2. **Transformer** – A class that accepts an upstream `Consumable` and returns a modified `Consumer`.
3. **Pipe** – Plain function application to glue them together.

### Core vs. Domain Packages

- **Is it a new behavioral strategy?** (e.g., a new backpressure or rate-limiting model) $\rightarrow$ _Open an issue to discuss adding it to the core library._
- **Is it a transport protocol or target side-effect?** (e.g., File I/O, WebSockets, DOM) $\rightarrow$ _This belongs in our scoped domain packages (like `@watchmanlab/io` or `@watchmanlab/dom`)._

---

## 🚀 How to Contribute

### 1. Code Style & Performance

We write raw, high-performance TypeScript. To preserve our V8 JIT monomorphic optimization path:

- **Prefer Classes over Closures:** Transformers should be implemented as classes extending `Source` to keep object shapes rigid and optimized for the engine.
- **Zero Allocations on the Hot Path:** Ensure your handler code avoids allocating transient objects or arrays inside the loop if those values are processed continuously.
- **Keep the Core Pure:** Do not inject error awareness, promises, or configurations into the foundational interfaces.

### 2. The Pull Request (PR) Workflow

We keep the pipeline moving fast:

- **Fork & Branch:** Create a branch named after your feature (e.g., `feat/my-transformer` or `fix/loop-edge-case`).
- **Test Your Credits:** Ensure any new transformer correctly propagates the credit (`.next()`) signals upstream and handles fallback buffering if the producer is non-cooperative.
- **Submit the PR:** Describe what your code does and why it helps performance. Don't worry about hyper-strict commit message formatting; we value clean code over rigid lint rules.

---

## 💡 Got an Idea or Bug Report?

If you find a broken edge case in the credit-draining loop or want to port the protocol to another language (like Rust or Go), open a GitHub Issue first so we can sync on the layout before you start coding!
