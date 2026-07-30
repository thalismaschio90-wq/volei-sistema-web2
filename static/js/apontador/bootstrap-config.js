(function (global) {
    "use strict";

    function getConfig() {
        const config = global.VTP_APONTADOR_BOOTSTRAP;
        if (!config || typeof config !== "object") {
            throw new Error("Configuração inicial do jogo do apontador não foi carregada.");
        }
        if (!config.partidaId || !config.competicao || !config.urls) {
            throw new Error("Configuração inicial do apontador está incompleta.");
        }
        return config;
    }

    global.VolleyTableProApontador = Object.freeze({ getConfig });
})(window);
