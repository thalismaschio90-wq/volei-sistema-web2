"use strict";

(function (global) {
    function normalizarNumeroScout(valor) {
        return String(valor ?? "").trim();
    }

    function numeroVersaoEstado(valor) {
        const numero = Number(valor || 0);
        return Number.isFinite(numero) && numero > 0 ? Math.trunc(numero) : 0;
    }

    function extrairVersaoEstado(dados) {
        if (!dados || typeof dados !== "object") return 0;
        return numeroVersaoEstado(
            dados.estado_versao
            ?? dados.versao_estado
            ?? dados.estado_versao_atual
            ?? dados.estado?.estado_versao
        );
    }

    function normalizarDataTempoMs(valor) {
        if (!valor) return 0;
        if (valor instanceof Date) return valor.getTime() || 0;
        let texto = String(valor || "").trim();
        if (!texto) return 0;
        if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(texto)) texto = texto.replace(" ", "T");
        const ms = Date.parse(texto);
        return Number.isFinite(ms) ? ms : 0;
    }

    function formatarDoisDigitos(numero) {
        return String(numero).padStart(2, "0");
    }

    function formatarDataHora(ms, tipo) {
        const data = new Date(ms || Date.now());
        if (tipo === "data") {
            return `${formatarDoisDigitos(data.getDate())}/${formatarDoisDigitos(data.getMonth() + 1)}/${data.getFullYear()}`;
        }
        return `${formatarDoisDigitos(data.getHours())}:${formatarDoisDigitos(data.getMinutes())}:${formatarDoisDigitos(data.getSeconds())}`;
    }

    function formatarDuracao(ms) {
        const totalSegundos = Math.max(0, Math.floor((ms || 0) / 1000));
        const horas = Math.floor(totalSegundos / 3600);
        const minutos = Math.floor((totalSegundos % 3600) / 60);
        const segundos = totalSegundos % 60;
        return horas > 0
            ? `${formatarDoisDigitos(horas)}:${formatarDoisDigitos(minutos)}:${formatarDoisDigitos(segundos)}`
            : `${formatarDoisDigitos(minutos)}:${formatarDoisDigitos(segundos)}`;
    }

    function limparNomeComparacao(valor) {
        return String(valor || "")
            .trim()
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .toLowerCase();
    }

    function numeroInteiro(valor, padrao = 0) {
        const numero = parseInt(valor, 10);
        return Number.isFinite(numero) ? numero : padrao;
    }

    global.VolleyTableProApontadorEstadoUtils = Object.freeze({
        normalizarNumeroScout,
        numeroVersaoEstado,
        extrairVersaoEstado,
        normalizarDataTempoMs,
        formatarDataHora,
        formatarDuracao,
        limparNomeComparacao,
        numeroInteiro
    });
})(window);
