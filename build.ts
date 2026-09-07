// Generate documentation
await Bun.$`bun run scripts/generate-docs.ts`;

// Clean dist
await Bun.$`rm -rf dist`;

// Build JavaScript
const result = await Bun.build({
  entrypoints: ["src/index.ts"],
  outdir: "./dist",
  target: "browser",
  format: "esm",
  splitting: true,
  sourcemap: "external",
  minify: true,
});

if (!result.success) {
  console.error("Build failed", result.logs);
  process.exit(1);
}
// Build TypeScript declarations
await Bun.$`bunx tsc --emitDeclarationOnly --allowImportingTsExtensions --noEmit false`;

export {};
