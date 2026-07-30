(function () {
    "use strict";

    function lerJSONLocal(chave, padrao) {
        try {
            const bruto = localStorage.getItem(chave);
            return bruto === null ? padrao : JSON.parse(bruto);
        } catch (erro) {
            return padrao;
        }
    }

    function salvarJSONLocal(chave, valor) {
        localStorage.setItem(chave, JSON.stringify(valor));
    }

    function carregarFila(chave) {
        const fila = lerJSONLocal(chave, []);
        return Array.isArray(fila) ? fila : [];
    }

    function salvarFila(chave, fila, aoSalvar) {
        const filaSegura = Array.isArray(fila) ? fila : [];
        salvarJSONLocal(chave, filaSegura);
        if (typeof aoSalvar === "function") {
            aoSalvar(filaSegura);
        }
        return filaSegura;
    }

    function adicionarFila({ chave, tipo, payload, setNumero, aoSalvar }) {
        const fila = carregarFila(chave);
        const idLocal = globalThis.crypto && typeof globalThis.crypto.randomUUID === "function"
            ? globalThis.crypto.randomUUID()
            : `${Date.now()}-${Math.random()}`;

        fila.push({
            id_local: idLocal,
            tipo,
            payload: { ...(payload || {}), set_numero: setNumero },
            set_numero: setNumero,
            criada_em: new Date().toISOString(),
            status: "pendente",
            tentativas: 0,
            ultimo_envio_em: null,
            confirmado_em: null
        });

        return salvarFila(chave, fila, aoSalvar);
    }


    function atualizarItens(chave, ids, alteracoes, aoSalvar) {
        const conjunto = new Set((ids || []).map(String));
        const agora = new Date().toISOString();
        const fila = carregarFila(chave).map(item => {
            if (!conjunto.has(String(item && item.id_local || ""))) return item;
            const patch = typeof alteracoes === "function" ? alteracoes(item, agora) : (alteracoes || {});
            return { ...item, ...patch };
        });
        return salvarFila(chave, fila, aoSalvar);
    }

    window.VTPOfflineStorage = Object.freeze({
        lerJSONLocal,
        salvarJSONLocal,
        carregarFila,
        salvarFila,
        adicionarFila,
        atualizarItens
    });
})();
