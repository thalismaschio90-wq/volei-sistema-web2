(function (global) {
    "use strict";

    function gerarComandoId() {
        if (global.crypto && typeof global.crypto.randomUUID === "function") {
            return global.crypto.randomUUID();
        }
        return `ponto-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function montarPayloadPonto(equipe, scout, contexto) {
        const dadosScout = scout || {};
        const ctx = contexto || {};
        return {
            comando_id: ctx.comandoId || gerarComandoId(),
            equipe,
            fundamento: dadosScout.fundamento || dadosScout.detalhe_lance || "",
            resultado: dadosScout.resultado || dadosScout.tipo_lance || "",
            tipo_lance: dadosScout.tipo_lance || dadosScout.resultado || "",
            detalhe_lance: dadosScout.detalhe_lance || dadosScout.fundamento || "",
            tipo_erro: dadosScout.tipo_erro || "",
            atleta_numero: dadosScout.atleta_numero || "",
            atleta_nome: dadosScout.atleta_nome || "",
            atleta_label: dadosScout.atleta_label || "",
            responsavel_lado: dadosScout.responsavel_lado || dadosScout.equipe_responsavel || "",
            saque_atual_local: ctx.saqueAtual || "",
            rotacao_a_local: Array.isArray(ctx.rotacaoA) ? [...ctx.rotacaoA] : [],
            rotacao_b_local: Array.isArray(ctx.rotacaoB) ? [...ctx.rotacaoB] : [],
            operador_dispositivo_id: ctx.dispositivoId || ""
        };
    }

    async function registrarPontoOficial(http, url, equipe, scout, contexto, opcoes) {
        if (!http || typeof http.enviarJson !== "function") {
            throw new Error("Camada HTTP indisponível para registrar ponto.");
        }
        return http.enviarJson(
            url,
            montarPayloadPonto(equipe, scout, contexto),
            { cache: "no-store", ...(opcoes || {}) }
        );
    }

    async function desfazerAcaoOficial(http, url) {
        if (!http || typeof http.requisitarJson !== "function") {
            throw new Error("Camada HTTP indisponível para desfazer ação.");
        }
        return http.requisitarJson(url, {
            method: "POST",
            headers: { "X-Requested-With": "fetch" }
        });
    }

    global.VTPPontosAPI = Object.freeze({
        gerarComandoId,
        montarPayloadPonto,
        registrarPontoOficial,
        desfazerAcaoOficial
    });
})(window);
