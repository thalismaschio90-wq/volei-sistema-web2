(function (global) {
    "use strict";

    function limiteTemposAtual(dados, numeroInteiro) {
        return numeroInteiro(dados?.limite_tempos ?? 2, 2);
    }

    function temposUsadosDoLado(lado, dados, estadoAtual, numeroInteiro) {
        const fonte = dados || estadoAtual || {};
        return numeroInteiro(lado === "A" ? fonte.tempos_a : fonte.tempos_b, 0);
    }

    function tempoRestanteDoLado(lado, dados, contexto) {
        const { estadoAtual, numeroInteiro, tempoRestanteA, tempoRestanteB } = contexto;
        if (dados && (dados.tempos_restantes_a !== undefined || dados.tempos_restantes_b !== undefined)) {
            return numeroInteiro(lado === "A" ? dados.tempos_restantes_a : dados.tempos_restantes_b, 0);
        }
        if (dados && (dados.tempos_a !== undefined || dados.tempos_b !== undefined)) {
            return Math.max(
                0,
                limiteTemposAtual(dados, numeroInteiro)
                    - temposUsadosDoLado(lado, dados, estadoAtual, numeroInteiro)
            );
        }
        if (lado === "A") return numeroInteiro(tempoRestanteA?.textContent, 0);
        return numeroInteiro(tempoRestanteB?.textContent, 0);
    }

    function podePedirTempo(lado, dados, contexto) {
        return !contexto.partidaFinalizada()
            && tempoRestanteDoLado(lado, dados, contexto) > 0;
    }

    function criarControladorCronometro(opcoes) {
        let intervaloTempo = null;
        let tempoAtual = 0;

        function emitirCronometro(equipe, status, segundos) {
            try {
                const socket = opcoes.obterSocket();
                if (socket && socket.connected) {
                    socket.emit("cronometro_tempo", {
                        partida_id: opcoes.partidaId,
                        competicao: opcoes.competicao,
                        equipe: equipe || "",
                        status,
                        ...(status === "iniciado"
                            ? { duracao: segundos, iniciado_em_ms: Date.now(), termina_em_ms: Date.now() + (segundos * 1000) }
                            : { restante: segundos, finalizado_em_ms: Date.now() }),
                        segundos
                    });
                }
            } catch (erro) {
                console.warn("Falha ao sincronizar cronômetro de tempo.", erro);
            }
        }

        function iniciar(segundos = 30, equipe = "") {
            tempoAtual = Math.max(0, opcoes.numeroInteiro(segundos, 30));
            const equipeNormalizada = opcoes.normalizarEquipe(equipe);
            const nomeEquipeTempo = (equipeNormalizada === "A" || equipeNormalizada === "B")
                ? opcoes.nomeEquipePorLado(equipeNormalizada)
                : String(equipe || "");

            if (!opcoes.cronometroEl) {
                console.error("Elemento #cronometro-tempo não encontrado.");
                return;
            }

            opcoes.cronometroEl.style.setProperty("display", "block", "important");
            opcoes.cronometroEl.textContent = "⏱️ " + tempoAtual;
            opcoes.atualizarMobileCronometro("⏱️ " + tempoAtual, true);
            opcoes.abrirPopupJogoGrande("tempo", {
                segundos: tempoAtual,
                nome: "CRONÔMETRO",
                equipe: nomeEquipeTempo ? `Tempo ${nomeEquipeTempo}` : "Tempo técnico"
            });

            emitirCronometro(equipe, "iniciado", tempoAtual);
            if (intervaloTempo) clearInterval(intervaloTempo);

            intervaloTempo = setInterval(() => {
                tempoAtual = Math.max(0, tempoAtual - 1);
                opcoes.cronometroEl.textContent = "⏱️ " + tempoAtual;
                opcoes.atualizarMobileCronometro("⏱️ " + tempoAtual, true);
                opcoes.atualizarPopupTempo(tempoAtual);
                // As demais telas contam localmente. Não envia um evento por segundo.
                if (tempoAtual <= 0) {
                    clearInterval(intervaloTempo);
                    intervaloTempo = null;
                    opcoes.cronometroEl.textContent = "⏱️ FIM DO TEMPO";
                    opcoes.atualizarMobileCronometro("⏱️ FIM", true);
                    opcoes.atualizarPopupTempo(0);
                    setTimeout(opcoes.fecharPopupJogoGrande, 1200);
                    emitirCronometro(equipe, "finalizado", 0);
                }
            }, 1000);
        }

        function parar() {
            if (intervaloTempo) clearInterval(intervaloTempo);
            intervaloTempo = null;
        }

        return { iniciar, parar };
    }

    async function registrarTempo(equipe, opcoes) {
        if (opcoes.partidaFinalizada()) return;
        opcoes.limparErro();

        if (!opcoes.podePedirTempo(equipe)) {
            opcoes.mostrarErro(`A equipe ${opcoes.nomeEquipePorLado(equipe)} não possui mais pedidos de tempo neste set.`);
            opcoes.atualizarTravasOperacionais();
            return;
        }

        await opcoes.enviarAcaoRapida("tempo", { equipe });
    }

    global.VTPTempos = Object.freeze({
        limiteTemposAtual,
        temposUsadosDoLado,
        tempoRestanteDoLado,
        podePedirTempo,
        criarControladorCronometro,
        registrarTempo
    });
})(window);
