

const CACHE_VERSION = 'v3';
const APP_CACHE = 'locationplus-app-' + CACHE_VERSION;
const CDN_CACHE = 'locationplus-cdn-' + CACHE_VERSION;
const TILE_CACHE = 'locationplus-tiles-v1';

const TILE_HOSTS = ['basemaps.cartocdn.com'];
const CDN_HOSTS = ['unpkg.com', 'fonts.googleapis.com', 'fonts.gstatic.com'];

const APP_SHELL = [
  '/',
  '/static/index.html',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Outfit:wght@300;400;500;600;700;800&display=swap',
];

self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(APP_CACHE);

    await Promise.all(APP_SHELL.map(async (url) => {
      try {
        const resp = await fetch(url, { cache: 'reload', credentials: 'omit' });
        if (resp && (resp.ok || resp.type === 'opaque')) {
          await cache.put(url, resp.clone());
        }
      } catch (_) {

      }
    }));
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keep = new Set([APP_CACHE, CDN_CACHE, TILE_CACHE]);
    const names = await caches.keys();
    await Promise.all(names.filter((n) => !keep.has(n)).map((n) => caches.delete(n)));
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  let url;
  try { url = new URL(req.url); } catch { return; }

  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;
  if (url.pathname === '/ws') return;

  if (req.mode === 'navigate') {
    event.respondWith(handleNavigation(req));
    return;
  }

  if (url.origin === self.location.origin && url.pathname.startsWith('/api/')) {
    return;
  }

  if (url.origin === self.location.origin && url.pathname.startsWith('/static/')) {
    event.respondWith(staleWhileRevalidate(req, APP_CACHE));
    return;
  }

  if (TILE_HOSTS.some((h) => url.hostname.includes(h))) {
    event.respondWith(cacheFirst(req, TILE_CACHE));
    return;
  }

  if (CDN_HOSTS.some((h) => url.hostname.includes(h))) {
    event.respondWith(staleWhileRevalidate(req, CDN_CACHE));
    return;
  }
});

async function handleNavigation(req) {
  try {
    const resp = await fetch(req);
    if (resp && resp.ok) {
      const cache = await caches.open(APP_CACHE);
      cache.put('/', resp.clone()).catch(() => {});
    }
    return resp;
  } catch (_) {
    const cache = await caches.open(APP_CACHE);
    const cached = (await cache.match('/'))
      || (await cache.match('/static/index.html'))
      || (await caches.match(req));
    if (cached) return cached;
    return new Response(
      '<!doctype html><meta charset=utf-8><title>Location+ offline</title>'
      + '<style>body{font-family:system-ui;background:#08070f;color:#eae7f5;'
      + 'display:grid;place-items:center;height:100vh;margin:0;text-align:center;padding:20px}</style>'
      + '<div><h1>Offline</h1><p>Location+ cache is empty. Reconnect and reload once to prime it.</p></div>',
      { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8' } },
    );
  }
}

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  if (cached) return cached;
  try {
    const resp = await fetch(req);
    if (resp && resp.ok) cache.put(req, resp.clone()).catch(() => {});
    return resp;
  } catch (_) {
    return new Response('', { status: 504, statusText: 'Offline' });
  }
}

async function staleWhileRevalidate(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(req);
  const network = fetch(req).then((resp) => {
    if (resp && (resp.ok || resp.type === 'opaque')) {
      cache.put(req, resp.clone()).catch(() => {});
    }
    return resp;
  }).catch(() => null);
  if (cached) {

    network.catch(() => {});
    return cached;
  }
  const fromNet = await network;
  if (fromNet) return fromNet;
  return new Response('', { status: 504, statusText: 'Offline' });
}

self.addEventListener('message', (event) => {
  const data = event.data || {};

  if (data.type === 'CACHE_TILES') {
    cacheTileBatch(data.urls, data.batchId).then((stats) => {
      self.clients.matchAll().then((clients) => {
        clients.forEach((c) => c.postMessage({
          type: 'CACHE_TILES_DONE', batchId: data.batchId, ...stats,
        }));
      });
    });
  }

  if (data.type === 'CACHE_COUNT') {
    caches.open(TILE_CACHE).then((cache) =>
      cache.keys().then((keys) => {
        if (event.ports && event.ports[0]) event.ports[0].postMessage({ count: keys.length });
      })
    ).catch(() => {
      if (event.ports && event.ports[0]) event.ports[0].postMessage({ count: 0 });
    });
  }

  if (data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

async function cacheTileBatch(urls, batchId) {
  const cache = await caches.open(TILE_CACHE);
  let cached = 0, failed = 0, skipped = 0;

  for (let i = 0; i < urls.length; i++) {
    try {
      const existing = await cache.match(urls[i]);
      if (existing) { skipped++; }
      else {
        const resp = await fetch(urls[i]);
        if (resp && resp.ok) { await cache.put(urls[i], resp); cached++; }
        else { failed++; }
      }
    } catch { failed++; }

    if ((i + 1) % 100 === 0 || i === urls.length - 1) {
      self.clients.matchAll().then((clients) => {
        clients.forEach((c) => c.postMessage({
          type: 'CACHE_PROGRESS',
          batchId,
          done: i + 1,
          total: urls.length,
          cached,
          skipped,
          failed,
        }));
      });
    }
  }
  return { cached, skipped, failed };
}
