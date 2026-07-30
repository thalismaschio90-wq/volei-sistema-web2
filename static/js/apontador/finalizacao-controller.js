(function (global) {
    "use strict";

    function criarFinalizacaoController(deps) {
        if (!deps || typeof deps !== "object") {
            throw new Error("Dependências da finalização não informadas.");
        }

        let observacoesAbrindo = false;

        function estadoConfirmaPartidaFinalizada(dados) {
            const estadoAtual = typeof deps.getEstadoAtual === "function" ? (deps.getEstadoAtual() || {}) : {};
            const regras = deps.regrasIniciais || {};
            const numeroInteiro = deps.numeroInteiro;
            const calcularAlvo = deps.setsParaVencerPelasRegras;
            dados = dados && typeof dados === "object" ? dados : {};

            const status = String(dados.status_jogo || dados.status || "").trim().toLowerCase();
            const fase = String(dados.fase_partida || "").trim().toLowerCase();
            const tipoEncerramento = String(dados.tipo_encerramento || "").trim().toLowerCase();
            const setsA = numeroInteiro(dados.sets_a ?? estadoAtual.sets_a ?? 0, 0);
            const setsB = numeroInteiro(dados.sets_b ?? estadoAtual.sets_b ?? 0, 0);
            const alvoFallback = calcularAlvo(dados);
            const alvo = numeroInteiro(
                dados.sets_para_vencer ?? estadoAtual.sets_para_vencer ?? regras.sets_para_vencer,
                alvoFallback
            );
            const statusFinal = status === "finalizada" || status === "encerrado" || fase === "encerrado";
            const placarFinal = Math.max(setsA, setsB) >= Math.max(1, alvo);
            const woFinal = tipoEncerramento === "wo" && statusFinal;
            const flagFinalExplicita = dados.partida_finalizada === true
                || dados.encerrado === true
                || dados.fim_jogo === true
                || dados.abrir_observacoes === true;

            return Boolean(flagFinalExplicita || (statusFinal && (placarFinal || woFinal)));
        }

        function limparPersistenciaLocal() {
            if (typeof deps.salvarFilaOffline === "function") deps.salvarFilaOffline([]);
            try {
                global.localStorage.removeItem(deps.chaveOperacaoLocal);
            } catch (erro) {
                // O jogo pode continuar em navegadores com armazenamento bloqueado.
            }
            if (typeof deps.removerJogoOfflineSeFinalizado === "function") {
                deps.removerJogoOfflineSeFinalizado();
            }
        }

        function obterPacoteOperacao() {
            try {
                return JSON.parse(global.localStorage.getItem(deps.chaveOperacaoLocal) || "{}");
            } catch (erro) {
                return {};
            }
        }

        function destinoObservacoes(dados) {
            return (dados && dados.url_observacoes) || deps.urlObservacoes;
        }

        async function abrirObservacoesAutomaticamente(dados) {
            const estado = dados || (typeof deps.getEstadoAtual === "function" ? deps.getEstadoAtual() : {});
            if (!estadoConfirmaPartidaFinalizada(estado)) return false;
            if (observacoesAbrindo) return false;
            observacoesAbrindo = true;

            try {
                const retornoHttp = await deps.http.enviarJson(deps.urlEncerrar, {
                    observacoes: "",
                    eventos: deps.carregarFilaOffline(),
                    estado_final: deps.montarEstadoManualParaBanco(),
                    pacote_operacao: obterPacoteOperacao()
                });
                const resposta = retornoHttp.resposta;
                const retorno = retornoHttp.dados || {};
                if (!resposta.ok || retorno.ok === false) {
                    throw new Error(retorno.mensagem || "Não foi possível salvar o encerramento.");
                }

                limparPersistenciaLocal();
                global.location.replace(destinoObservacoes(retorno));
                return true;
            } catch (erro) {
                observacoesAbrindo = false;
                deps.mostrarErro(
                    (erro && erro.message)
                    || "Falha ao salvar a partida final. A fila continua guardada neste dispositivo."
                );
                return false;
            }
        }

        function redirecionarObservacoesOficiais(dados, atrasoMs) {
            observacoesAbrindo = true;
            limparPersistenciaLocal();
            const atraso = Number.isFinite(Number(atrasoMs)) ? Number(atrasoMs) : 120;
            global.setTimeout(function () {
                global.location.replace(destinoObservacoes(dados || {}));
            }, Math.max(0, atraso));
        }

        return Object.freeze({
            estadoConfirmaPartidaFinalizada,
            abrirObservacoesAutomaticamente,
            redirecionarObservacoesOficiais,
            estaAbrindoObservacoes: function () { return observacoesAbrindo; }
        });
    }

    global.VTPFinalizacaoController = Object.freeze({ criar: criarFinalizacaoController });
})(window);
