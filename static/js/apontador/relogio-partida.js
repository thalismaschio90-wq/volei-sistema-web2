(function (global) {
    "use strict";

    function criar(opcoes) {
        const cfg = opcoes || {};
        const storage = cfg.storage || global.localStorage;
        const chaveStorage = String(cfg.chaveStorage || "");
        const normalizarDataTempoMs = cfg.normalizarDataTempoMs;
        const formatarDataHora = cfg.formatarDataHora;
        const formatarDuracao = cfg.formatarDuracao;
        const obterEstado = typeof cfg.obterEstado === "function" ? cfg.obterEstado : function () { return {}; };

        let inicioPartidaRealMs = normalizarDataTempoMs(cfg.inicioPartidaReal);
        let fimPartidaRealMs = normalizarDataTempoMs(cfg.fimPartidaReal);
        let intervalo = null;

        function carregarLocal() {
            if (!storage || !chaveStorage) return;
            try {
                const salvo = JSON.parse(storage.getItem(chaveStorage) || "{}");
                if (!inicioPartidaRealMs && salvo.inicioPartidaRealMs) {
                    inicioPartidaRealMs = Number(salvo.inicioPartidaRealMs) || 0;
                }
                if (!fimPartidaRealMs && salvo.fimPartidaRealMs) {
                    fimPartidaRealMs = Number(salvo.fimPartidaRealMs) || 0;
                }
            } catch (erro) {
                // Um valor local inválido não pode interromper a operação da partida.
            }
        }

        function salvarLocal() {
            if (!storage || !chaveStorage) return;
            try {
                storage.setItem(chaveStorage, JSON.stringify({
                    inicioPartidaRealMs: inicioPartidaRealMs || 0,
                    fimPartidaRealMs: fimPartidaRealMs || 0
                }));
            } catch (erro) {
                // Falha de armazenamento local não deve bloquear placar ou apontamento.
            }
        }

        function atualizar() {
            const agoraMs = Date.now();
            const baseMs = inicioPartidaRealMs || agoraMs;
            const fimOuAgora = fimPartidaRealMs || agoraMs;
            const duracao = inicioPartidaRealMs ? formatarDuracao(fimOuAgora - baseMs) : "00:00";

            if (cfg.relogioDataEl) cfg.relogioDataEl.textContent = formatarDataHora(agoraMs, "data");
            if (cfg.relogioHoraEl) cfg.relogioHoraEl.textContent = formatarDataHora(agoraMs, "hora");
            if (cfg.relogioDuracaoEl) cfg.relogioDuracaoEl.textContent = `⏱ ${duracao}`;
            if (cfg.mobileRelogioEl) {
                cfg.mobileRelogioEl.textContent = inicioPartidaRealMs ? `⏱ ${duracao}` : "⏱ 00:00";
            }
        }

        function garantirInicio() {
            if (!inicioPartidaRealMs) {
                inicioPartidaRealMs = Date.now();
                const estado = obterEstado();
                if (estado && typeof estado === "object") {
                    estado.inicio_partida_real = new Date(inicioPartidaRealMs).toISOString();
                }
                salvarLocal();
            }
            atualizar();
        }

        function aplicarBackend(dados) {
            if (!dados || typeof dados !== "object") return;
            const inicioMs = normalizarDataTempoMs(
                dados.inicio_partida_real || dados.inicio_partida || dados.data_hora_inicio_real
            );
            const fimMs = normalizarDataTempoMs(
                dados.fim_partida_real || dados.fim_partida || dados.data_hora_fim_real
            );
            if (inicioMs) inicioPartidaRealMs = inicioMs;
            if (fimMs) fimPartidaRealMs = fimMs;
            if (inicioMs || fimMs) salvarLocal();
        }

        function iniciarAtualizacao() {
            carregarLocal();
            atualizar();
            if (!intervalo) intervalo = global.setInterval(atualizar, 1000);
        }

        function pararAtualizacao() {
            if (!intervalo) return;
            global.clearInterval(intervalo);
            intervalo = null;
        }

        return Object.freeze({
            aplicarBackend,
            atualizar,
            garantirInicio,
            iniciarAtualizacao,
            pararAtualizacao
        });
    }

    global.ApontadorRelogioPartida = Object.freeze({ criar });
})(window);
