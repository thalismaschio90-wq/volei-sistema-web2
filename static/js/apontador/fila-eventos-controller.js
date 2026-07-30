(function () {
    "use strict";

    function criarFilaEventosController(config) {
        const cfg = config || {};
        const storage = cfg.storage;
        const http = cfg.http;

        if (!storage) throw new Error("offline-storage.js é obrigatório para a fila de eventos.");
        if (!http) throw new Error("api-http.js é obrigatório para a fila de eventos.");

        let sincronizacaoSetEmAndamento = false;

        function inteiro(valor, padrao = 0) {
            return typeof cfg.numeroInteiro === "function"
                ? cfg.numeroInteiro(valor, padrao)
                : (Number.isFinite(Number(valor)) ? Number.parseInt(valor, 10) : padrao);
        }

        function descricao(tipo, payload) {
            const dados = payload || {};
            const equipe = dados.equipe || "";
            if (tipo === "tempo") return `Tempo confirmado - Equipe ${equipe}`;
            if (tipo === "substituicao") return `Equipe ${equipe} • substituição • ${dados.numero_sai}>${dados.numero_entra}`;
            if (tipo === "substituicao_excepcional") return `Equipe ${equipe} • substituição excepcional • ${dados.numero_sai}>${dados.numero_entra}`;
            if (tipo === "retardamento") return `Equipe ${equipe} • retardamento`;
            if (tipo === "sancao") return `Equipe ${equipe} • sanção • ${dados.tipo_sancao || dados.sancao || ""} • ${dados.nome || dados.numero || dados.alvo || ""}`;
            if (tipo === "cartao_verde") return `Equipe ${equipe} • cartão verde • ${dados.nome || dados.numero || dados.alvo || ""}`;
            return "Ação registrada";
        }

        function carregar() {
            return storage.carregarFila(cfg.chaveFila);
        }

        function aoSalvar(fila) {
            if (typeof cfg.aoSalvar === "function") cfg.aoSalvar(fila);
        }

        function salvar(fila) {
            return storage.salvarFila(cfg.chaveFila, fila, aoSalvar);
        }

        function adicionar(tipo, payload) {
            const setNumero = inteiro(
                typeof cfg.obterSetAtual === "function" ? cfg.obterSetAtual() : 1,
                1
            );
            return storage.adicionarFila({
                chave: cfg.chaveFila,
                tipo,
                payload,
                setNumero,
                aoSalvar
            });
        }

        async function sincronizarSet(setNumero, usarKeepalive = false) {
            const numeroSet = inteiro(setNumero, 0);
            if (!numeroSet || sincronizacaoSetEmAndamento || !navigator.onLine) return false;

            const lote = carregar().filter(item =>
                inteiro(item?.set_numero ?? item?.payload?.set_numero, 0) === numeroSet
            );
            if (!lote.length) return true;

            sincronizacaoSetEmAndamento = true;
            const idsLote = lote.map(item => String(item?.id_local || "")).filter(Boolean);
            if (typeof storage.atualizarItens === "function") {
                storage.atualizarItens(cfg.chaveFila, idsLote, (item, agora) => ({
                    status: "enviando",
                    tentativas: Number(item?.tentativas || 0) + 1,
                    ultimo_envio_em: agora
                }), aoSalvar);
            }
            try {
                const { resposta, dados } = await http.enviarJson(cfg.urlSincronizar, {
                    set_numero: numeroSet,
                    eventos: lote
                }, { keepalive: !!usarKeepalive });

                if (!resposta.ok || dados.ok === false) {
                    throw new Error(dados.mensagem || "Falha ao sincronizar set.");
                }

                const confirmados = new Set((dados.eventos_confirmados || []).map(String));
                if (confirmados.size) {
                    if (typeof storage.atualizarItens === "function") {
                        storage.atualizarItens(cfg.chaveFila, [...confirmados], { status: "confirmado", confirmado_em: new Date().toISOString() }, aoSalvar);
                    }
                    salvar(carregar().filter(item => !confirmados.has(String(item?.id_local || ""))));
                }
                return true;
            } catch (erro) {
                if (typeof storage.atualizarItens === "function") {
                    storage.atualizarItens(cfg.chaveFila, idsLote, { status: "pendente" }, aoSalvar);
                }
                console.warn(`Set ${numeroSet} ficou pendente para nova tentativa.`, erro);
                return false;
            } finally {
                sincronizacaoSetEmAndamento = false;
            }
        }

        async function sincronizarTudo() {
            const sets = [...new Set(
                carregar()
                    .map(item => inteiro(item?.set_numero ?? item?.payload?.set_numero, 0))
                    .filter(Boolean)
            )];
            for (const numeroSet of sets) await sincronizarSet(numeroSet, false);
        }

        function registrarReconexao() {
            window.addEventListener("online", sincronizarTudo);
        }

        return Object.freeze({
            descricao,
            carregar,
            salvar,
            adicionar,
            sincronizarSet,
            sincronizarTudo,
            registrarReconexao
        });
    }

    window.VTPFilaEventos = Object.freeze({ criarFilaEventosController });
})();
