```javascript
const CACHE_NAME = "voleitable-pwa-v20260607-tablet1";
const OFFLINE_URL = "/offline-apontador?v=20260607-tablet1";

const APP_SHELL = [
    "/app-login?app=1&v=20260607-tablet1",
    OFFLINE_URL,
    "/static/css/app_login.css?v=20260607-tablet1",
    "/static/js/app_login.js?v=20260607-tablet1",
    "/static/img/logo.png?v=20260607-tablet1",
    "/manifest.json?v=20260607-tablet1"
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
            .then(keys => Promise.all(
                keys.map(key => key !== CACHE_NAME ? caches.delete(key) : null)
            ))
            .then(() => self.clients.claim())
    );
});

async function colocarNoCache(request, response) {
    try {
        if (!response || !response.ok) return;
        const cache = await caches.open(CACHE_NAME);
        await cache.put(request, response.clone());
    } catch (e) {}
}

function deveIgnorar(url) {
    return (
        url.pathname.includes("/socket.io/") ||
        url.pathname.includes("/auth/") ||
        url.pathname.includes("/login") ||
        url.pathname.includes("/logout")
    );
}

function ehPaginaDoSistema(url) {
    return (
        url.pathname.includes("/apontador") ||
        url.pathname.includes("/papeleta") ||
        url.pathname.includes("/partida") ||
        url.pathname.includes("/placar") ||
        url.pathname.includes("/arbitro") ||
        url.pathname.includes("/treinador") ||
        url.pathname.includes("/offline-apontador") ||
        url.pathname === "/app" ||
        url.pathname === "/app-login"
    );
}

self.addEventListener("message", event => {
    const data = event.data || {};

    if (data.type === "CACHE_URLS" && Array.isArray(data.urls)) {
        event.waitUntil(
            caches.open(CACHE_NAME).then(async cache => {
                for (const rawUrl of data.urls) {
                    try {
                        const url = new URL(rawUrl, self.location.origin).toString();
                        const request = new Request(url, { credentials: "include", cache: "no-store" });
                        const response = await fetch(request);

                        if (response && response.ok) {
                            await cache.put(url, response.clone());
                        }

                    } catch (e) {
                        console.log("Falha ao cachear URL offline:", rawUrl, e);
                    }
                }
            })
        );
    }

    if (data.type === "SKIP_WAITING") {
        self.skipWaiting();
    }
});

self.addEventListener("fetch", event => {
    const request = event.request;

    if (request.method !== "GET") return;

    const url = new URL(request.url);

    if (deveIgnorar(url)) return;

    if (request.mode === "navigate") {

        event.respondWith(
            fetch(request, {
                cache: "default",
                credentials: "include"
            })
            .then(response => {

                if (
                    response &&
                    response.ok &&
                    ehPaginaDoSistema(url)
                ) {
                    colocarNoCache(request, response.clone());
                }

                return response;
            })
            .catch(async () => {

                const cache = await caches.open(CACHE_NAME);

                const cachedPage = await cache.match(request);

                if (cachedPage) {
                    return cachedPage;
                }

                const cachedUrl = await cache.match(url.pathname);

                if (cachedUrl) {
                    return cachedUrl;
                }

                return (
                    cache.match(OFFLINE_URL) ||
                    cache.match("/app-login?app=1&v=20260607-tablet1")
                );
            })
        );

        return;
    }

    if (
        url.pathname.endsWith(".css") ||
        url.pathname.endsWith(".js") ||
        url.pathname.endsWith(".json") ||
        url.pathname.endsWith(".png") ||
        url.pathname.endsWith(".jpg") ||
        url.pathname.endsWith(".jpeg") ||
        url.pathname.endsWith(".webp") ||
        url.pathname.endsWith(".svg")
    ) {

        event.respondWith(
            fetch(request, {
                cache: "default"
            })
            .then(response => {

                colocarNoCache(request, response.clone());

                return response;
            })
            .catch(() => caches.match(request))
        );

        return;
    }

    event.respondWith(
        caches.match(request)
            .then(cached => {

                if (cached) {
                    return cached;
                }

                return fetch(request)
                    .then(response => {

                        if (
                            response &&
                            response.ok &&
                            ehPaginaDoSistema(url)
                        ) {
                            colocarNoCache(request, response.clone());
                        }

                        return response;
                    });

            })
            .catch(() => caches.match(OFFLINE_URL))
    );
});
```
