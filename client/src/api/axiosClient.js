// src/api/axiosClient.js
import axios from 'axios';
import { getGlobalCsrfToken, setGlobalCsrfToken } from '../context/CsrfContext';

const axiosClient = axios.create({
  baseURL: process.env.REACT_APP_API_BASE_URL,
  withCredentials: true,
  headers: { 'X-Requested-With': 'XMLHttpRequest' }
});

// one-shot inflight promise so concurrent calls don't spam the endpoint
let csrfPromise = null;

async function ensureCsrfToken(baseURL) {
  if (getGlobalCsrfToken()) return getGlobalCsrfToken();
  if (!csrfPromise) {
    csrfPromise = axios.get(`${baseURL}/api/get-csrf-token`, {
      withCredentials: true,
      headers: { 'X-Requested-With': 'XMLHttpRequest' }
    }).then(res => {
      setGlobalCsrfToken(res.data.csrfToken);
      return res.data.csrfToken;
    }).finally(() => { csrfPromise = null; });
  }
  return csrfPromise;
}

axiosClient.interceptors.request.use(async (config) => {
  const method = (config.method || 'get').toLowerCase();
  const isUnsafe = ['post','put','patch','delete'].includes(method);

  // Normalize path against baseURL
  const url = (config.url || '');
  const path = url.startsWith('http') ? new URL(url).pathname : url;

  // 1) Never require CSRF for /auth/* or the token endpoint itself
  const skip = path.startsWith('/auth/') || path === '/api/get-csrf-token';

  if (isUnsafe && !skip) {
    let csrf = getGlobalCsrfToken();
    if (!csrf) {
      // fetch token on-demand (works only after login for /api/*)
      csrf = await ensureCsrfToken(config.baseURL || '');
    }
    config.headers['X-CSRF-Token'] = csrf;
  }

  return config;
});

// // Optional: auto-refresh CSRF and retry once on 403 invalid csrf token
// axiosClient.interceptors.response.use(
//   r => r,
//   async (error) => {
//     const { config, response } = error || {};
//     if (response?.status === 403 && !config?._csrfRetried) {
//       try {
//         await ensureCsrfToken(axiosClient.defaults.baseURL || '');
//         config._csrfRetried = true;
//         config.headers = { ...(config.headers || {}), 'X-CSRF-Token': getGlobalCsrfToken() };
//         return axiosClient(config);
//       } catch (e) {
//         // fall through
//       }
//     }
//     return Promise.reject(error);
//   }
// );

export default axiosClient;
