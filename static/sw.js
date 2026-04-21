// Bump this version every time you deploy a significant update.
// Changing it forces ALL browsers to drop the old cache immediately.
const CACHE = 'genrri-v130';
const DATA_CACHE = 'genrri-data-v2';

// Only cache third-party CDN assets (charts library etc.) — never the app HTML/JS.
const CDN_CACHE = [
  'https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js',
  'https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js'
];

// API endpoints to cache for offline viewing (read-only GET endpoints)
const CACHEABLE_API = ['/api/portfolio', '/api/transactions', '/api/watchlist', '/api/portfolio-history', '/api/portfolio-history-intraday', '/api/settings', '/api/financials', '/api/analyst', '/api/corporate-actions', '/api/holders', '/api/alerts'];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE)
      .then(c => c.addAll(CDN_CACHE))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  // Delete every cache that isn't current
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE && k !== DATA_CACHE).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = e.request.url;

  // For cacheable API GET requests — network first, cache fallback (offline mode)
  // Only cache successful (200 OK) responses — never cache errors or rate-limit responses
  if (url.includes('/api/') && e.request.method === 'GET' && CACHEABLE_API.some(p => url.includes(p))) {
    e.respondWith(
      fetch(e.request)
        .then(response => {
          if (response.ok) {  // Only cache 200-299 responses
            const clone = response.clone();
            caches.open(DATA_CACHE).then(cache => cache.put(e.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // Never intercept other API calls (POST, mutations)
  if (url.includes('/api/')) return;

  // For the app HTML — always network first, only cache on success
  const pathname = new URL(url).pathname;
  if (pathname === '/' || pathname.endsWith('/') || url.includes('index.html') || url.includes('sw.js') || url.includes('manifest.json')) {
    e.respondWith(
      fetch(e.request)
        .then(response => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE).then(cache => cache.put(e.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // For static assets and CDN — cache first, network fallback
  if (e.request.method === 'GET') {
    e.respondWith(
      caches.match(e.request).then(cached => {
        if (cached) return cached;
        return fetch(e.request).then(response => {
          const clone = response.clone();
          caches.open(CACHE).then(cache => cache.put(e.request, clone));
          return response;
        });
      })
    );
  }
});
