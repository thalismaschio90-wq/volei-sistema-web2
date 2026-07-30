(function (global) {
    "use strict";

    function criarDependencias(opcoes) {
        const deps = opcoes || {};
        return {
            ultimaAcaoEl: deps.ultimaAcaoEl || null,
            historicoAcoesEl: deps.historicoAcoesEl || null,
            mobileUltimaAcaoEl: deps.mobileUltimaAcaoEl || null,
            documentRef: deps.documentRef || global.document
        };
    }

    function criar(opcoes) {
        const deps = criarDependencias(opcoes);

        function atualizarUltimaAcao(texto) {
            const finalTexto = String(texto || "-").trim() || "-";
            const mensagem = "ÚLTIMA AÇÃO: " + finalTexto;
            if (deps.ultimaAcaoEl) deps.ultimaAcaoEl.textContent = mensagem;
            if (deps.mobileUltimaAcaoEl) deps.mobileUltimaAcaoEl.textContent = mensagem;
        }

        function criarItemHistorico(texto) {
            const item = deps.documentRef.createElement("div");
            item.className = "historico-item";
            item.textContent = texto;
            return item;
        }

        function renderHistoricoBackend(lista) {
            const container = deps.historicoAcoesEl;
            if (!container) return;

            container.innerHTML = "";

            if (!Array.isArray(lista) || !lista.length) {
                container.appendChild(criarItemHistorico("Nenhuma ação registrada."));
                return;
            }

            const filtrada = lista.filter((acao) => {
                const texto = String((typeof acao === "string" ? acao : (acao?.descricao || acao?.mensagem || "")) || "").toLowerCase();
                const tipo = String((acao && typeof acao === "object" ? (acao.tipo || acao.tipo_evento || "") : "") || "").toLowerCase();
                if (["tempo", "substituicao", "substituição", "substituicao_excepcional"].includes(tipo)) return false;
                if (texto.includes("tempo") || texto.includes("substituição") || texto.includes("substituicao") || texto.includes("solicitou")) return false;
                return true;
            }).slice(0, 5);

            if (!filtrada.length) {
                container.appendChild(criarItemHistorico("Nenhuma ação de ponto registrada."));
                return;
            }

            filtrada.forEach((acao) => {
                const texto = typeof acao === "string"
                    ? acao
                    : (acao.descricao || acao.mensagem || JSON.stringify(acao));
                container.appendChild(criarItemHistorico(texto));
            });
        }

        function alternarTelaCheia() {
            const doc = deps.documentRef;
            if (!doc.fullscreenElement) {
                const promessa = doc.documentElement?.requestFullscreen?.();
                if (promessa && typeof promessa.catch === "function") promessa.catch(() => {});
                return;
            }
            const promessa = doc.exitFullscreen?.();
            if (promessa && typeof promessa.catch === "function") promessa.catch(() => {});
        }

        return Object.freeze({
            atualizarUltimaAcao,
            renderHistoricoBackend,
            alternarTelaCheia
        });
    }

    global.ApontadorRenderUI = Object.freeze({ criar });
})(window);
