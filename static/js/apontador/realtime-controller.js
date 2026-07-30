(function (global) {
    "use strict";

    function criarController(opcoes = {}) {
        const socket = opcoes.socket || null;
        const socketSync = opcoes.socketSync || null;
        const numeroInteiro = typeof opcoes.numeroInteiro === "function"
            ? opcoes.numeroInteiro
            : ((valor, padrao = 0) => Number.isFinite(Number(valor)) ? Math.trunc(Number(valor)) : padrao);

        const solicitacoesRecebidas = new Set();
        const alertaEl = document.getElementById("alerta-solicitacao");
        const alertaTextoEl = document.getElementById("alerta-solicitacao-texto");
        let alertaTimer = null;

        function valor(nome, padrao) {
            const getter = opcoes[nome];
            if (typeof getter !== "function") return padrao;
            try {
                const resultado = getter();
                return resultado === undefined ? padrao : resultado;
            } catch (_) {
                return padrao;
            }
        }

        function montarPayload(extra = {}) {
            const estado = valor("getEstadoAtual", {}) || {};
            const saqueAtual = String(valor("getSaqueAtual", estado.saque_atual || "") || "").toUpperCase();
            const rotacoes = valor("getRotacoes", {}) || {};
            const infoSaque = valor("getInfoSacador", {}) || {};
            const versao = numeroInteiro(valor("getEstadoVersao", 0), 0);
            const disciplina = valor("getDisciplina", {}) || {};

            return {
                ...estado,
                ...extra,
                ok: true,
                origem: extra.origem || "apontador_local",
                partida_id: String(opcoes.partidaId || ""),
                competicao: String(opcoes.competicao || ""),
                operador_login: String(opcoes.operadorLogin || ""),
                apontador_login: String(opcoes.operadorLogin || ""),
                operador_sessao_token: String(opcoes.operadorSessaoToken || ""),
                operador_dispositivo_id: String(opcoes.dispositivoId || ""),
                equipe_a: String(opcoes.equipeA || ""),
                equipe_b: String(opcoes.equipeB || ""),
                pontos_a: numeroInteiro(estado.pontos_a ?? estado.placar_a ?? 0, 0),
                pontos_b: numeroInteiro(estado.pontos_b ?? estado.placar_b ?? 0, 0),
                placar_a: numeroInteiro(estado.pontos_a ?? estado.placar_a ?? 0, 0),
                placar_b: numeroInteiro(estado.pontos_b ?? estado.placar_b ?? 0, 0),
                sets_a: numeroInteiro(estado.sets_a ?? 0, 0),
                sets_b: numeroInteiro(estado.sets_b ?? 0, 0),
                set_atual: numeroInteiro(estado.set_atual ?? 1, 1),
                saque_atual: saqueAtual,
                sacador_nome: extra.sacador_nome ?? infoSaque.nome ?? estado.sacador_nome ?? "",
                sacador_numero: extra.sacador_numero ?? infoSaque.numero ?? estado.sacador_numero ?? "",
                rotacao_a: Array.isArray(rotacoes.A) ? rotacoes.A : [],
                rotacao_b: Array.isArray(rotacoes.B) ? rotacoes.B : [],
                ultima_acao: extra.ultima_acao || estado.ultima_acao || "Atualização em tempo real",
                tempos_a: numeroInteiro(estado.tempos_a ?? 0, 0),
                tempos_b: numeroInteiro(estado.tempos_b ?? 0, 0),
                subs_a: numeroInteiro(estado.subs_a ?? 0, 0),
                subs_b: numeroInteiro(estado.subs_b ?? 0, 0),
                sancoes_a: Array.isArray(estado.sancoes_a) ? estado.sancoes_a : (disciplina.sancoesA || []),
                sancoes_b: Array.isArray(estado.sancoes_b) ? estado.sancoes_b : (disciplina.sancoesB || []),
                cartoes_vermelhos_a: Array.isArray(estado.cartoes_vermelhos_a) ? estado.cartoes_vermelhos_a : (disciplina.vermelhosA || []),
                cartoes_vermelhos_b: Array.isArray(estado.cartoes_vermelhos_b) ? estado.cartoes_vermelhos_b : (disciplina.vermelhosB || []),
                vinculos_substituicao: estado.vinculos_substituicao || { A: {}, B: {} },
                substituidos_finalizados: estado.substituidos_finalizados || { A: [], B: [] },
                cartoes_verdes_a: Array.isArray(estado.cartoes_verdes_a) ? estado.cartoes_verdes_a : (disciplina.verdesA || []),
                cartoes_verdes_b: Array.isArray(estado.cartoes_verdes_b) ? estado.cartoes_verdes_b : (disciplina.verdesB || []),
                timestamp_local: Date.now(),
                estado_versao_base: versao,
                estado_versao: versao,
                scout_por_lado: valor("getScout", {}),
                lados_invertidos_apontador: !!valor("getLadosInvertidos", false),
                lados_invertidos: !!valor("getLadosInvertidos", false),
                quadra_invertida: !!valor("getLadosInvertidos", false)
            };
        }

        function emitirEstado(extra = {}) {
            if (valor("getTransicaoSetEmAndamento", false) && !extra.transicao_set) return false;
            if (!socket || !socket.connected || !socketSync) return false;
            return socketSync.emitir(socket, "estado_partida_local", montarPayload(extra));
        }

        function chaveSolicitacao(dados) {
            if (!dados || typeof dados !== "object") return "";
            if (dados.id_solicitacao) return String(dados.id_solicitacao);
            return [dados.tipo || "", dados.equipe || "", dados.equipe_nome || "", dados.mensagem || ""].join("|");
        }

        function mostrarAlertaSolicitacao(texto) {
            if (!alertaEl || !alertaTextoEl) return;
            alertaTextoEl.textContent = texto || "Nova solicitação recebida.";
            alertaEl.style.display = "block";
            if (alertaTimer) clearTimeout(alertaTimer);
            alertaTimer = setTimeout(() => {
                alertaEl.style.display = "none";
            }, 6500);
        }

        function receberSolicitacao(dados) {
            if (!dados || typeof dados !== "object") return;

            const tipo = String(dados.tipo || "").trim().toLowerCase();
            const status = String(dados.status || "").trim().toLowerCase();
            const origem = String(dados.origem || "").trim().toLowerCase();
            const ehPedidoPendente = ["tempo", "substituicao"].includes(tipo)
                && !["confirmada", "confirmado", "executada", "executado", "iniciado", "finalizado", "recusada", "recusado"].includes(status)
                && (origem.includes("treinador") || dados.id_solicitacao);
            if (!ehPedidoPendente) return;

            const chave = chaveSolicitacao(dados);
            if (chave && solicitacoesRecebidas.has(chave)) return;
            if (chave) {
                solicitacoesRecebidas.add(chave);
                setTimeout(() => solicitacoesRecebidas.delete(chave), 30000);
            }

            const equipe = String(dados.equipe || "").toUpperCase();
            const equipeNome = String(dados.equipe_nome || equipe || "").trim();
            const base = tipo === "substituicao" ? "Pedido de substituição" : "Pedido de tempo";
            mostrarAlertaSolicitacao(String(dados.mensagem || `${base} - ${equipeNome}`).trim());
        }

        function ehEcoDoProprioApontador(dados) {
            if (!dados || typeof dados !== "object") return false;
            const mesmaPartida = String(dados.partida_id || "") === String(opcoes.partidaId || "");
            const mesmoDispositivo = dados.operador_dispositivo_id
                && String(dados.operador_dispositivo_id) === String(opcoes.dispositivoId || "");
            const mesmaSessao = dados.operador_sessao_token
                && String(dados.operador_sessao_token) === String(opcoes.operadorSessaoToken || "");
            const origemLocal = String(dados.origem || "").toLowerCase().includes("apontador_local");
            return mesmaPartida && origemLocal && (mesmoDispositivo || mesmaSessao);
        }

        return Object.freeze({
            montarPayload,
            emitirEstado,
            receberSolicitacao,
            ehEcoDoProprioApontador
        });
    }

    global.VTPApontadorRealtimeController = Object.freeze({ criarController });
})(window);
