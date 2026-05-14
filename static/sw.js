const CACHE_NAME = "voleitable-v1";

const urlsToCache = [
    "/",
    "/login",
    "/static/css/landing.css"
];

self.addEventListener("install", event => {

    self.skipWaiting();

    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(urlsToCache))
    );

});

self.addEventListener("activate", event => {

    event.waitUntil(
        caches.keys().then(keys => {
            return Promise.all(
                keys.map(key => {
                    if (key !== CACHE_NAME) {
                        return caches.delete(key);
                    }
                })
            );
        })
    );

});

self.addEventListener("fetch", event => {

    if (event.request.mode === "navigate") {

        event.respondWith(
            fetch(event.request)
                .catch(() => caches.match("/"))
        );

        return;
    }

    event.respondWith(
        caches.match(event.request)
            .then(response => {
                return response || fetch(event.request);
            })
    );

});