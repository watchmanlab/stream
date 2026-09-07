// Clean dist
await Bun.$`rm -rf dist`;

// Build JavaScript
const result = await Bun.build({
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
await Bun.$`bunx tsc --emitDeclarationOnly --allowImportingTsExtensions --noEmit false`;

export {};
