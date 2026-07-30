(function (global) {
    "use strict";

    const REGISTROS = new WeakMap();

    function uuid() {
        if (global.crypto && typeof global.crypto.randomUUID === "function") return global.crypto.randomUUID();
        return `hb-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function iniciar(socket, opcoes) {
        if (!socket || typeof socket.on !== "function" || typeof socket.emit !== "function") return null;
        parar(socket);
        const cfg = { intervaloMs: 15000, timeoutMs: 10000, ...(opcoes || {}) };
        let timer = null;
        let timeout = null;
        let ultimoEnvio = 0;
        let ultimoAck = 0;
        let latenciaMs = null;

        function limparTimeout() {
            if (timeout) clearTimeout(timeout);
            timeout = null;
        }

        function enviar() {
            if (!socket.connected) return false;
            ultimoEnvio = Date.now();
            const heartbeatId = uuid();
            socket.emit("cliente_heartbeat", {
                heartbeat_id: heartbeatId,
                cliente_enviado_em_ms: ultimoEnvio,
                partida_id: String(cfg.partidaId || ""),
                competicao: String(cfg.competicao || ""),
                perfil: String(cfg.perfil || "desconhecido"),
                cliente_id: String(cfg.clienteId || ""),
                estado_versao: Number(typeof cfg.obterVersao === "function" ? cfg.obterVersao() : 0) || 0
            });
            limparTimeout();
            timeout = setTimeout(() => {
                if (ultimoAck < ultimoEnvio && typeof cfg.aoTimeout === "function") cfg.aoTimeout();
            }, Math.max(2000, Number(cfg.timeoutMs) || 10000));
            return true;
        }

        function aoAck(dados) {
            ultimoAck = Date.now();
            latenciaMs = ultimoEnvio ? Math.max(0, ultimoAck - ultimoEnvio) : null;
            limparTimeout();
            if (typeof cfg.aoAck === "function") cfg.aoAck(dados || {}, latenciaMs);
        }

        function aoConnect() {
            enviar();
            if (!timer) timer = setInterval(enviar, Math.max(5000, Number(cfg.intervaloMs) || 15000));
        }

        function aoDisconnect() {
            limparTimeout();
            if (typeof cfg.aoDesconectar === "function") cfg.aoDesconectar();
        }

        socket.on("connect", aoConnect);
        socket.on("disconnect", aoDisconnect);
        socket.on("cliente_heartbeat_ok", aoAck);
        if (socket.connected) aoConnect();

        const controle = {
            enviar,
            status: () => ({ ultimoEnvio, ultimoAck, latenciaMs }),
            destruir: () => parar(socket),
            handlers: { aoConnect, aoDisconnect, aoAck },
            get timer() { return timer; },
            set timer(v) { timer = v; },
            limparTimeout
        };
        REGISTROS.set(socket, controle);
        return controle;
    }

    function parar(socket) {
        const atual = REGISTROS.get(socket);
        if (!atual) return;
        if (atual.timer) clearInterval(atual.timer);
        atual.limparTimeout();
        if (typeof socket.off === "function") {
            socket.off("connect", atual.handlers.aoConnect);
            socket.off("disconnect", atual.handlers.aoDisconnect);
            socket.off("cliente_heartbeat_ok", atual.handlers.aoAck);
        }
        REGISTROS.delete(socket);
    }

    global.VTPRealtimeHeartbeat = Object.freeze({ iniciar, parar });
})(window);
