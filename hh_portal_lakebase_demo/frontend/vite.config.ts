import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Build straight into the app's static dir; the bundle syncs app/static (gitignored).
export default defineConfig({
  plugins: [react()],
  build: { outDir: "../app/static", emptyOutDir: true },
  server: { proxy: { "/api": "http://localhost:8000" } },
});
