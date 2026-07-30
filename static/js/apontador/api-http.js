"use strict";

(function () {
    async function lerJsonSeguro(resposta, fallback = {}) {
        if (!resposta || typeof resposta.json !== "function") return fallback;
        try {
            return await resposta.json();
        } catch (e) {
            return fallback;
        }
    }

    async function requisitarJson(url, opcoes = {}, fallback = {}) {
        const resposta = await fetch(url, opcoes);
        const dados = await lerJsonSeguro(resposta, fallback);
        return { resposta, dados };
    }

    async function enviarJson(url, payload, opcoes = {}) {
        const headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "fetch",
            ...(opcoes.headers || {})
        };
        return requisitarJson(url, {
            method: opcoes.method || "POST",
            ...opcoes,
            headers,
            body: JSON.stringify(payload ?? {})
        });
    }

    function enviarJsonSemAguardar(url, payload, opcoes = {}) {
        try {
            return fetch(url, {
                method: opcoes.method || "POST",
                ...opcoes,
                headers: {
                    "Content-Type": "application/json",
                    "X-Requested-With": "fetch",
                    ...(opcoes.headers || {})
                },
                body: JSON.stringify(payload ?? {})
            }).catch(() => null);
        } catch (e) {
            return Promise.resolve(null);
        }
    }

    window.VolleyTableProApontadorApi = Object.freeze({
        lerJsonSeguro,
        requisitarJson,
        enviarJson,
        enviarJsonSemAguardar
    });
})();
