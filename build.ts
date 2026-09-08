import { $, build } from "bun";

// Clean dist
await $`rm -rf dist`;

// Build JavaScript
const result = await build({
  entrypoints: ["./index.ts"],
  outdir: "./dist",
  target: "node",
  format: "esm",
  sourcemap: true,
  minify: true,
});

if (!result.success) {
  console.error("Build failed", result.logs);
  process.exit(1);
}
// Build TypeScript declarations
await $`bunx tsc --emitDeclarationOnly --allowImportingTsExtensions --noEmit false`;

export {};
