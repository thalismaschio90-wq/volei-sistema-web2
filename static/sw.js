const CACHE_NAME = "voleitable-app-v20260519-final";

const APP_SHELL = [
    "/app-login?v=20260519-final",
    "/static/css/app_login.css?v=20260519-final",
    "/static/js/app_login.js?v=20260519-final",
    "/static/img/logo.png?v=20260519-final",
    "/static/icons/icon-192.png?v=20260519-final",
    "/static/icons/icon-512.png?v=20260519-final"
];

self.addEventListener("install", event => {
    self.skipWaiting();

    event.waitUntil(
        caches
            .open(CACHE_NAME)
            .then(cache => cache.addAll(APP_SHELL).catch(() => null))
    );
});

self.addEventListener("activate", event => {
    event.waitUntil(
        caches
            .keys()
            .then(keys => Promise.all(
                keys.map(key => {
                    if (key !== CACHE_NAME) {
                        return caches.delete(key);
                    }

                    return null;
                })
            ))
            .then(() => self.clients.claim())
    );
});

self.addEventListener("fetch", event => {
    const req = event.request;

    if (req.mode === "navigate") {
        event.respondWith(
            fetch(req)
                .then(response => {
                    const clone = response.clone();

                    caches
                        .open(CACHE_NAME)
                        .then(cache => cache.put(req, clone))
                        .catch(() => null);

                    return response;
                })
                .catch(() => caches.match("/app-login?v=20260519-final"))
        );

        return;
    }

    event.respondWith(
        fetch(req)
            .then(response => {
                const clone = response.clone();

                caches
                    .open(CACHE_NAME)
                    .then(cache => cache.put(req, clone))
                    .catch(() => null);

                return response;
            })
            .catch(() => caches.match(req))
    );
});
