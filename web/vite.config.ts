import { defineConfig } from "vite";

// Dev: `npm run dev` serves on :5173 and proxies the game socket to the
// gameserver on :8080, so the page and /ws share an origin (no CORS for WS).
// The API (Envoy :10000) and Keycloak (:8180) are called cross-origin; both
// allow http://localhost:5173 (backend/internal/api/v2.go, keycloak realm).
export default defineConfig({
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      "/ws": { target: "ws://localhost:8080", ws: true },
      "/status": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
    chunkSizeWarningLimit: 900, // three.js is ~600 KB minified
  },
});
