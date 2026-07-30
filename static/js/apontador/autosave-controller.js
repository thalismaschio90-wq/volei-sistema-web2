(function (global) {
    "use strict";

    function criarAutosaveController(opcoes) {
        const cfg = opcoes || {};
        const intervaloMinimo = Number(cfg.intervaloMinimoMs || 7000);
        let timer = null;
        let ultimoEnvioEm = 0;
        let envioEmAndamento = false;
        let ultimoHash = "";

        function hashLeveEstado(estado) {
            try {
                return JSON.stringify({
                    pontos_a: estado?.pontos_a ?? estado?.placar_a ?? 0,
                    pontos_b: estado?.pontos_b ?? estado?.placar_b ?? 0,
                    sets_a: estado?.sets_a ?? 0,
                    sets_b: estado?.sets_b ?? 0,
                    set_atual: estado?.set_atual ?? 1,
                    saque_atual: estado?.saque_atual ?? "",
                    rotacao_a: estado?.rotacao_a ?? [],
                    rotacao_b: estado?.rotacao_b ?? [],
                    tempos_a: estado?.tempos_a ?? 0,
                    tempos_b: estado?.tempos_b ?? 0,
                    subs_a: estado?.subs_a ?? 0,
                    subs_b: estado?.subs_b ?? 0,
                    ultima_acao: estado?.ultima_acao ?? "",
                    fila_tamanho: cfg.carregarFila().length
                });
            } catch (erro) {
                return String(Date.now());
            }
        }

        function salvarSnapshotLocal(motivo = "autosave") {
            try {
                if (!cfg.estadoInicialSincronizado() && motivo !== "acao_local_confirmada") return null;
                const estado = cfg.montarEstado();
                const pacote = {
                    ok: true,
                    partida_id: cfg.partidaId,
                    competicao: cfg.competicao,
                    equipe_a: cfg.equipeA,
                    equipe_b: cfg.equipeB,
                    motivo,
                    pendente_banco: true,
                    atualizado_em: new Date().toISOString(),
                    estado,
                    fila_offline: cfg.carregarFila()
                };
                cfg.salvarJSON(cfg.chaveAutosave, pacote);
                if (cfg.offlineHabilitado) cfg.salvarJSON(cfg.chaveEstadoOffline, estado);
                return pacote;
            } catch (erro) {
                return null;
            }
        }

        async function enviarHeartbeat() {
            if (!cfg.urlHeartbeat || !cfg.tokenOperador) return true;
            try {
                const { resposta, dados } = await cfg.http.requisitarJson(cfg.urlHeartbeat, {
                    method: "POST",
                    headers: {
                        "X-Requested-With": "fetch",
                        "X-Operador-Sessao": String(cfg.tokenOperador || ""),
                        "X-Dispositivo-Operacao": String(cfg.dispositivoId || "")
                    },
                    keepalive: true
                });
                if (!resposta.ok) {
                    const mensagem = dados.mensagem || "Esta partida está aberta em outro dispositivo ou navegador.";
                    try { alert(mensagem); } catch (erro) {}
                    global.location.href = cfg.urlPainelCompeticao;
                    return false;
                }
                return true;
            } catch (erro) {
                return false;
            }
        }

        function iniciarHeartbeat(intervaloMs = 15000) {
            enviarHeartbeat();
            return global.setInterval(enviarHeartbeat, intervaloMs);
        }

        async function enviarSnapshotBanco({ pausar = false, finalizar = false, motivo = "autosave", forcar = false } = {}) {
            if (cfg.transicaoSetEmAndamento() && !finalizar) return false;
            if (envioEmAndamento && !forcar) return false;

            const pacoteLocal = salvarSnapshotLocal(motivo);
            if (!pacoteLocal || !cfg.urlSalvarEstado) return false;

            const estado = pacoteLocal.estado || cfg.montarEstado();
            const hashAtual = hashLeveEstado(estado);
            const agora = Date.now();

            if (!forcar) {
                if (hashAtual === ultimoHash) return true;
                if (agora - ultimoEnvioEm < intervaloMinimo) return false;
            }

            envioEmAndamento = true;
            ultimoEnvioEm = agora;
            ultimoHash = hashAtual;

            try {
                const { resposta, dados } = await cfg.http.enviarJson(cfg.urlSalvarEstado, {
                    pausar,
                    finalizar,
                    autosave: true,
                    motivo,
                    estado
                }, {
                    headers: {
                        "X-Operador-Sessao": String(cfg.tokenOperador || ""),
                        "X-Dispositivo-Operacao": String(cfg.dispositivoId || "")
                    },
                    keepalive: true
                });
                if (resposta.ok && dados && dados.ok) {
                    const confirmado = cfg.lerJSON(cfg.chaveAutosave, {});
                    confirmado.pendente_banco = false;
                    confirmado.confirmado_banco_em = new Date().toISOString();
                    cfg.salvarJSON(cfg.chaveAutosave, confirmado);
                    return true;
                }
            } catch (erro) {
                // Mantém o snapshot local para nova tentativa.
            } finally {
                envioEmAndamento = false;
            }
            return false;
        }

        function agendar(delay = 1200, motivo = "autosave") {
            salvarSnapshotLocal(motivo);
            if (timer) return;
            timer = global.setTimeout(() => {
                timer = null;
                enviarSnapshotBanco({ motivo }).catch(() => {});
            }, Math.max(250, delay));
        }

        function enviarSaidaRapida({ pausar = true, finalizar = false, motivo = "salvar_e_sair" } = {}) {
            const pacoteLocal = salvarSnapshotLocal(motivo);
            const estado = pacoteLocal?.estado || cfg.montarEstado();
            const dados = { pausar, finalizar, autosave: true, saida_rapida: true, motivo, estado };
            const payload = JSON.stringify(dados);
            let enviadoBeacon = false;

            try {
                if (global.navigator.sendBeacon) {
                    const blob = new Blob([payload], { type: "application/json" });
                    enviadoBeacon = global.navigator.sendBeacon(cfg.urlSalvarEstado, blob);
                }
            } catch (erro) {}

            try {
                cfg.http.enviarJsonSemAguardar(cfg.urlSalvarEstado, dados, { keepalive: true });
            } catch (erro) {}
            return enviadoBeacon;
        }

        return Object.freeze({
            hashLeveEstado,
            salvarSnapshotLocal,
            enviarHeartbeat,
            iniciarHeartbeat,
            enviarSnapshotBanco,
            agendar,
            enviarSaidaRapida
        });
    }

    global.VTPAutosave = Object.freeze({ criarAutosaveController });
})(window);
