// Service Worker for Weather Predict Dashboard
// Provides offline caching for the app shell and API responses

const CACHE_NAME = 'weather-predict-v2';
const SHELL_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
];

// Install: cache app shell
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_ASSETS))
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch: network-first for API and HTML, cache-first only for sub-assets
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // API: network-first with cache fallback (offline resilience)
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(
      fetch(req).then((response) => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(req, clone));
        }
        return response;
      }).catch(() => caches.match(req))
    );
    return;
  }

  // HTML / navigation / app shell: network-first so dashboard updates
  // propagate on the next visit without manual cache busting.
  const isHTML = req.mode === 'navigate'
    || url.pathname === '/'
    || url.pathname.endsWith('.html')
    || url.pathname === '/manifest.json';
  if (isHTML) {
    event.respondWith(
      fetch(req).then((response) => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(req, clone));
        }
        return response;
      }).catch(() => caches.match(req))
    );
    return;
  }

  // Other sub-resources: cache-first
  event.respondWith(
    caches.match(req).then((cached) => cached || fetch(req))
  );
});
