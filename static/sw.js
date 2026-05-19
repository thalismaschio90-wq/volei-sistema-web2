const CACHE_NAME = "voleitable-pwa-v20260519-pwa3";

const APP_SHELL = [
    "/app-login?app=1&v=20260519-pwa3",

    "/static/css/app_login.css?v=20260519-pwa3",
    "/static/js/app_login.js?v=20260519-pwa3",

    "/static/img/logo.png?v=20260519-pwa3",

    "/manifest.json?v=20260519-pwa3"
];


// ============================================================
// 🔥 INSTALL
// ============================================================
self.addEventListener("install", event => {

    self.skipWaiting();

    event.waitUntil(

        caches
            .open(CACHE_NAME)
            .then(cache => {

                console.log("📦 Cache inicial criado");

                return cache.addAll(APP_SHELL);

            })
            .catch(error => {

                console.log(
                    "❌ Erro ao salvar cache inicial:",
                    error
                );

            })

    );

});


// ============================================================
// 🔥 ACTIVATE
// ============================================================
self.addEventListener("activate", event => {

    event.waitUntil(

        caches
            .keys()
            .then(keys => {

                return Promise.all(

                    keys.map(key => {

                        if (key !== CACHE_NAME) {

                            console.log(
                                "🗑️ Removendo cache antigo:",
                                key
                            );

                            return caches.delete(key);

                        }

                        return null;

                    })

                );

            })
            .then(() => {

                console.log("✅ Service Worker ativado");

                return self.clients.claim();

            })

    );

});


// ============================================================
// 🔥 FETCH
// ============================================================
self.addEventListener("fetch", event => {

    const request = event.request;

    // 🔥 IGNORA MÉTODOS DIFERENTES DE GET
    if (request.method !== "GET") {
        return;
    }

    // 🔥 NAVEGAÇÃO HTML
    if (request.mode === "navigate") {

        event.respondWith(

            fetch(request)

                .then(response => {

                    const clone = response.clone();

                    caches
                        .open(CACHE_NAME)
                        .then(cache => {

                            cache.put(request, clone);

                        })
                        .catch(() => null);

                    return response;

                })

                .catch(() => {

                    return caches.match(
                        "/app-login?app=1&v=20260519-pwa3"
                    );

                })

        );

        return;
    }

    // 🔥 CSS / JS / IMG
    event.respondWith(

        fetch(request)

            .then(response => {

                const clone = response.clone();

                caches
                    .open(CACHE_NAME)
                    .then(cache => {

                        cache.put(request, clone);

                    })
                    .catch(() => null);

                return response;

            })

            .catch(() => {

                return caches.match(request);

            })

    );

});