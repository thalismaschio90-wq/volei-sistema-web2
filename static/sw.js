const CACHE_NAME = "voleitable-pwa-v20260527-fix2";

const APP_SHELL = [
    "/app-login?app=1&v=20260527-fix2",
    "/static/css/app_login.css?v=20260527-fix2",
    "/static/js/app_login.js?v=20260527-fix2",
    "/static/img/logo.png?v=20260527-fix2",
    "/manifest.json?v=20260527-fix2"
];

self.addEventListener("install", event => {
    self.skipWaiting();

    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(APP_SHELL))
            .catch(error => {
                console.log("Erro ao criar cache inicial:", error);
            })
    );
});

self.addEventListener("activate", event => {
    event.waitUntil(
        caches.keys()
            .then(keys => {
                return Promise.all(
                    keys.map(key => {
                        if (key !== CACHE_NAME) {
                            return caches.delete(key);
                        }
                        return null;
                    })
                );
            })
            .then(() => self.clients.claim())
    );
});

self.addEventListener("fetch", event => {
    const request = event.request;

    if (request.method !== "GET") {
        return;
    }

    const url = new URL(request.url);

    if (url.pathname.includes("/socket.io/")) {
        return;
    }

    if (url.pathname.includes("/auth/") || url.pathname.includes("/login")) {
        return;
    }

    if (request.mode === "navigate") {
        event.respondWith(
            fetch(request, { cache: "no-store" })
                .then(response => response)
                .catch(() => caches.match("/app-login?app=1&v=20260527-fix2"))
        );
        return;
    }

    if (
        url.pathname.endsWith(".css") ||
        url.pathname.endsWith(".js") ||
        url.pathname.endsWith(".json")
    ) {
        event.respondWith(
            fetch(request, { cache: "no-store" })
                .then(response => {
                    const clone = response.clone();

                    caches.open(CACHE_NAME)
                        .then(cache => cache.put(request, clone))
                        .catch(() => null);

                    return response;
                })
                .catch(() => caches.match(request))
        );
        return;
    }

    event.respondWith(
        caches.match(request)
            .then(cached => {
                return cached || fetch(request)
                    .then(response => {
                        const clone = response.clone();

                        caches.open(CACHE_NAME)
                            .then(cache => cache.put(request, clone))
                            .catch(() => null);

                        return response;
                    });
            })
            .catch(() => fetch(request))
    );
});