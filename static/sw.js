const CACHE_NAME = "volleypwa-v1";

const ASSETS = [
    "/",
    "/static/manifest.json",
];

self.addEventListener("install", (event) => {

    event.waitUntil(

        caches.open(CACHE_NAME).then((cache) => {
            return cache.addAll(ASSETS);
        })

    );

    self.skipWaiting();

});

self.addEventListener("activate", (event) => {

    event.waitUntil(

        caches.keys().then((keys) => {

            return Promise.all(

                keys.map((key) => {

                    if (key !== CACHE_NAME) {
                        return caches.delete(key);
                    }

                })

            );

        })

    );

    self.clients.claim();

});

self.addEventListener("fetch", (event) => {

    // NÃO intercepta APIs/socket/estado
    if (
        event.request.url.includes("/estado")
        || event.request.url.includes("/socket.io")
        || event.request.method !== "GET"
    ) {
        return;
    }

    event.respondWith(

        caches.match(event.request).then((response) => {

            return (
                response ||
                fetch(event.request)
            );

        })

    );

});