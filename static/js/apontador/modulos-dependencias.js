(function (global) {
    "use strict";

    const MODULOS_OBRIGATORIOS = Object.freeze([
        "VolleyTableProApontador",
        "VolleyTableProApontadorEstadoUtils",
        "ApontadorRelogioPartida",
        "ApontadorRenderUI",
        "VTPApontadorSocketSync",
        "VTPApontadorRealtimeController",
        "VTPRealtimeHeartbeat",
        "VolleyTableProApontadorApi",
        "VTPPontosAPI",
        "VTPRotacaoUtils",
        "VTPTempos",
        "VTPSubstituicoes",
        "VTPSubstituicaoExcepcional",
        "VTPSancoesController",
        "VTPScoutController",
        "VTPFinalizacaoController",
        "VTPOfflineStorage",
        "VTPFilaEventos",
        "VTPOfflineGame",
        "VTPAutosave"
    ]);

    function listarAusentes() {
        return MODULOS_OBRIGATORIOS.filter((nome) => {
            const modulo = global[nome];
            return !modulo || (typeof modulo !== "object" && typeof modulo !== "function");
        });
    }

    function validar() {
        const ausentes = listarAusentes();
        if (ausentes.length) {
            throw new Error(
                "Módulos obrigatórios do apontador não foram carregados: " + ausentes.join(", ")
            );
        }
        return true;
    }

    global.VTPModulosApontador = Object.freeze({
        obrigatorios: MODULOS_OBRIGATORIOS,
        listarAusentes,
        validar
    });
})(window);
