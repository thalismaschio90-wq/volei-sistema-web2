(function (global) {
    "use strict";

    const REGISTROS_APONTADOR = new WeakMap();
    const ULTIMO_ESTADO_LEGADO = new WeakMap();

    const CONFIG_PADRAO = Object.freeze({
        transports: ["websocket", "polling"],
        upgrade: false,
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 500,
        randomizationFactor: 0.5,
        reconnectionDelayMax: 2000,
        timeout: 5000,
        forceNew: true
    });

    function criarSocket(ioFactory, opcoes = {}) {
        if (typeof ioFactory !== "function") return null;
        return ioFactory({ ...CONFIG_PADRAO, ...opcoes });
    }

    function registrarEventos(socket, eventos) {
        if (!socket || !eventos || typeof eventos !== "object") return false;
        Object.entries(eventos).forEach(([nome, handler]) => {
            if (typeof handler === "function") socket.on(nome, handler);
        });
        return true;
    }

    function removerEventos(socket, eventos) {
        if (!socket || typeof socket.off !== "function" || !Array.isArray(eventos)) return;
        eventos.forEach(({ nome, handler }) => {
            if (nome && typeof handler === "function") socket.off(nome, handler);
        });
    }

    function emitir(socket, evento, payload) {
        if (!socket || !socket.connected || !evento) return false;
        try {
            socket.emit(evento, payload);
            return true;
        } catch (_) {
            return false;
        }
    }

    function registrarHandlersApontador(socket, handlers = {}) {
        if (!socket || typeof socket.on !== "function") return false;

        const anteriores = REGISTROS_APONTADOR.get(socket) || [];
        removerEventos(socket, anteriores);

        const registros = [];
        const chamar = (nome, ...args) => {
            const handler = handlers[nome];
            if (typeof handler === "function") handler(...args);
        };
        const registrar = (nome, handler) => {
            socket.on(nome, handler);
            registros.push({ nome, handler });
        };

        // Conexão e confirmação do estado enviado pelo próprio apontador.
        registrar("connect", () => chamar("aoConectar"));
        registrar("disconnect", (motivo) => chamar("aoDesconectar", motivo));
        registrar("connect_error", (erro) => chamar("aoErroConexao", erro));
        registrar("estado_partida_local_ok", (dados) => chamar("aoConfirmarEstadoLocal", dados));

        // Compatibilidade RC1: os três nomes podem carregar o mesmo snapshot.
        // Deduplica por partida/versão para não renderizar três vezes o mesmo
        // estado quando o servidor mantém eventos legados em paralelo.
        const encaminharEstadoLegado = (dados) => {
            if (!dados || typeof dados !== "object") return;
            const partida = String(dados.partida_id || "");
            const versao = Number(dados.estado_versao || 0);
            const assinatura = versao > 0
                ? `${partida}:${versao}`
                : `${partida}:${dados.set_atual || 0}:${dados.pontos_a || 0}:${dados.pontos_b || 0}:${dados.ultima_acao || ""}`;
            const agora = Date.now();
            const anterior = ULTIMO_ESTADO_LEGADO.get(socket);
            if (anterior && anterior.assinatura === assinatura && (agora - anterior.em) < 1500) return;
            ULTIMO_ESTADO_LEGADO.set(socket, { assinatura, em: agora });
            chamar("aoReceberEstado", dados);
        };
        registrar("estado_partida", encaminharEstadoLegado);
        registrar("estado_jogo_atualizado", encaminharEstadoLegado);
        registrar("estado_partida_tempo_real", encaminharEstadoLegado);

        registrar("estado_partida_delta", (dados) => chamar("aoReceberDelta", dados));
        registrar("recuperacao_partida", (dados) => chamar("aoRecuperarPartida", dados));

        registrar("solicitacao_treinador", (dados) => chamar("aoReceberSolicitacao", dados));
        registrar("tempo_executado", (dados) => chamar("aoExecutarTempo", dados));
        registrar("tempo_apontador", (dados) => chamar("aoExecutarTempo", dados));
        registrar("tempo_oficial", (dados) => chamar("aoExecutarTempo", dados));
        registrar("cronometro_tempo", (dados) => chamar("aoAtualizarCronometro", dados));
        registrar("cronometro_arbitros", (dados) => chamar("aoAtualizarCronometro", dados));

        REGISTROS_APONTADOR.set(socket, registros);
        return true;
    }

    global.VTPApontadorSocketSync = Object.freeze({
        CONFIG_PADRAO,
        criarSocket,
        registrarEventos,
        registrarHandlersApontador,
        emitir
    });
})(window);
