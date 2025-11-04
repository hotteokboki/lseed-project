
import react from "@vitejs/plugin-react-swc";
import { defineConfig, loadEnv } from "vite";


export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target = (env.VITE_API_URL || "http://localhost:4000").trim();

  return {
    plugins: [react()],
    server: {
      port: 3000,
      open: true,
      strictPort: true,
      // Proxy common API routes to your Node/Express server
      proxy: {
        "/api": {
          target,
          changeOrigin: true,
          secure: false // keep false if using https self-signed locally
        },
        "/auth": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/login/2fa-verify": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/login": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/logout": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/apply-as-mentor": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/signup": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/signup-lseed-role": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/webhook-bot1": {
          target,
          changeOrigin: true,
          secure: false
        },
        "/lseed/googleform-webhook": {
          target,
          changeOrigin: true,
          secure: false
        },
        // add more if you have them, e.g.:
        // "/socket.io": { target, ws: true, changeOrigin: true, secure: false }
      }
    },
    preview: {
      port: 3000,
      strictPort: true
    }
  };
});
