import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

/**
 * Production: UI is served by Python on web_port (same origin).
 * Dev: only /api is proxied. WebSocket goes straight to the bot (see App.tsx wsUrl) — Vite's
 * ws proxy often throws "ws proxy socket error" when the backend closes the socket.
 */
export default defineConfig({
  plugins: [react()],
  server: {
    // Listen on all interfaces so http://127.0.0.1:5173 works (not only "localhost" / IPv6).
    host: true,
    port: 5173,
    strictPort: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8765",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyDir: true,
  },
});
