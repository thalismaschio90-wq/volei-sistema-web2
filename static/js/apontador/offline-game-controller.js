(function (global) {
    "use strict";

    function criarOfflineGameController(config) {
        const cfg = config || {};

        function habilitado() {
            return !!(typeof cfg.habilitado === "function" ? cfg.habilitado() : cfg.habilitado);
        }

        function montarPacote() {
            if (!habilitado()) return null;
            const estado = (typeof cfg.obterEstado === "function" ? cfg.obterEstado() : {}) || {};
            const rotacaoA = typeof cfg.obterRotacaoA === "function" ? cfg.obterRotacaoA() : [];
            const rotacaoB = typeof cfg.obterRotacaoB === "function" ? cfg.obterRotacaoB() : [];
            const scout = typeof cfg.obterScout === "function" ? cfg.obterScout() : {};
            const fila = typeof cfg.carregarFila === "function" ? cfg.carregarFila() : [];
            const numeroInteiro = cfg.numeroInteiro || ((valor, padrao) => Number.isFinite(Number(valor)) ? Number(valor) : padrao);
            const urlAtual = global.location ? global.location.href : "";

            return {
                id: cfg.partidaId,
                partida_id: cfg.partidaId,
                competicao: cfg.competicao,
                equipe_a: cfg.equipeA,
                equipe_b: cfg.equipeB,
                url: urlAtual,
                url_jogo: urlAtual,
                estado: {
                    ...estado,
                    pontos_a: numeroInteiro(estado?.pontos_a ?? estado?.placar_a ?? 0, 0),
                    pontos_b: numeroInteiro(estado?.pontos_b ?? estado?.placar_b ?? 0, 0),
                    sets_a: numeroInteiro(estado?.sets_a ?? 0, 0),
                    sets_b: numeroInteiro(estado?.sets_b ?? 0, 0),
                    set_atual: numeroInteiro(estado?.set_atual ?? 1, 1),
                    saque_atual: (typeof cfg.obterSaque === "function" ? cfg.obterSaque() : "") || estado?.saque_atual || "",
                    rotacao_a: Array.isArray(rotacaoA) ? [...rotacaoA] : [],
                    rotacao_b: Array.isArray(rotacaoB) ? [...rotacaoB] : [],
                    scout_local: scout,
                    fila_offline: fila
                },
                baixada_em: new Date().toISOString()
            };
        }

        async function cachearUrls() {
            if (!habilitado() || !("serviceWorker" in navigator)) return;
            try {
                const reg = await navigator.serviceWorker.ready;
                const alvo = (reg && reg.active) || navigator.serviceWorker.controller;
                if (alvo) {
                    alvo.postMessage({
                        type: "CACHE_URLS",
                        urls: [
                            "/offline-apontador?v=20260528-offline1",
                            "/app-login?app=1&v=20260528-offline1",
                            global.location ? global.location.href : ""
                        ].filter(Boolean)
                    });
                }
            } catch (erro) {
                console.warn("Não foi possível preparar o cache offline do jogo.", erro);
            }
        }

        function salvarLocal(silencioso = true) {
            if (!habilitado()) return false;
            const pacote = montarPacote();
            if (!pacote) return false;

            cfg.salvarJSON(cfg.chaveEstado, pacote.estado);
            const partidas = cfg.lerJSON(cfg.chavePartidas, []);
            const filtradas = (Array.isArray(partidas) ? partidas : []).filter(
                p => String(p.id || p.partida_id) !== String(cfg.partidaId)
            );
            filtradas.push({
                id: cfg.partidaId,
                partida_id: cfg.partidaId,
                competicao: cfg.competicao,
                equipe_a: cfg.equipeA,
                equipe_b: cfg.equipeB,
                url: pacote.url,
                url_jogo: pacote.url_jogo,
                baixada_em: pacote.baixada_em
            });
            cfg.salvarJSON(cfg.chavePartidas, filtradas);
            cfg.salvarJSON(cfg.chaveSessao, {
                autorizado: true,
                tipo: "apontador",
                nome: cfg.operadorNome || "",
                competicao: cfg.competicao,
                criada_em: new Date().toISOString()
            });

            cachearUrls();
            const botao = typeof cfg.obterBotao === "function" ? cfg.obterBotao() : null;
            if (botao) {
                botao.textContent = "✅ Offline salvo";
                botao.style.background = "#15803d";
            }
            if (!silencioso) global.alert("Jogo salvo para uso offline neste dispositivo.");
            return true;
        }

        function removerSeFinalizado() {
            if (!habilitado()) return;
            if (!(typeof cfg.partidaFinalizada === "function" && cfg.partidaFinalizada())) return;
            const partidas = cfg.lerJSON(cfg.chavePartidas, []);
            cfg.salvarJSON(
                cfg.chavePartidas,
                (Array.isArray(partidas) ? partidas : []).filter(
                    p => String(p.id || p.partida_id) !== String(cfg.partidaId)
                )
            );
            global.localStorage.removeItem(cfg.chaveEstado);
        }

        return {
            montarPacote,
            cachearUrls,
            salvarLocal,
            removerSeFinalizado
        };
    }

    global.VTPOfflineGame = { criarOfflineGameController };
})(window);
