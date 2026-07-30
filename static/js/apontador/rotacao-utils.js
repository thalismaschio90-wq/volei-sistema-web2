(function (global) {
    "use strict";

    const ROTACAO_VAZIA = Object.freeze(["", "", "", "", "", ""]);

    function numeroAtletaOperacional(valor) {
        if (valor && typeof valor === "object") {
            valor = valor.numero || valor.camisa || valor.numero_camisa || valor.atleta_numero || valor.n || "";
        }
        return String(valor ?? "").trim();
    }

    function normalizarAtletaOperacional(atleta, numeroFallback = "") {
        const base = (atleta && typeof atleta === "object") ? { ...atleta } : {};
        const numero = numeroAtletaOperacional(
            base.numero || base.camisa || base.numero_camisa || base.atleta_numero || numeroFallback
        );
        if (!numero) return null;
        base.numero = numero;
        base.camisa = base.camisa || numero;
        base.numero_camisa = base.numero_camisa || numero;
        base.nome = String(base.nome || base.atleta_nome || "Atleta").trim() || "Atleta";
        return base;
    }

    function mesclarAtletasComRotacao(atletas, rotacao) {
        const saida = [];
        const vistos = new Set();

        function adicionar(item, numeroFallback = "") {
            const atleta = normalizarAtletaOperacional(item, numeroFallback);
            if (!atleta || !atleta.numero || vistos.has(atleta.numero)) return;
            vistos.add(atleta.numero);
            saida.push(atleta);
        }

        (Array.isArray(atletas) ? atletas : []).forEach((atleta) => adicionar(atleta));
        (Array.isArray(rotacao) ? rotacao : []).forEach((numero) => {
            adicionar({ numero, nome: "Atleta" }, numero);
        });

        return saida;
    }

    function montarFallbackRotacaoSeguro(rotacaoBase, atletasBase) {
        const base = Array.isArray(rotacaoBase)
            ? rotacaoBase.map((numero) => String(numero || "").trim())
            : [];
        if (base.length === 6 && base.some((numero) => numero !== "")) return base;

        const numeros = [];
        const vistos = new Set();
        (Array.isArray(atletasBase) ? atletasBase : []).forEach((atleta) => {
            const numero = numeroAtletaOperacional(atleta);
            if (numero && !vistos.has(numero)) {
                vistos.add(numero);
                numeros.push(numero);
            }
        });

        while (numeros.length < 6) numeros.push("");
        return numeros.slice(0, 6);
    }

    function rotacaoValida(rotacao) {
        return Array.isArray(rotacao) && rotacao.length === 6;
    }

    function normalizarRotacao(rotacao, fallback, atual) {
        if (rotacaoValida(rotacao)) {
            const limpa = rotacao.map((numero) => numero ? String(numero) : "");
            if (limpa.some((numero) => numero !== "")) return limpa;
        }

        if (rotacaoValida(atual)) {
            const atualLimpa = atual.map((numero) => numero ? String(numero) : "");
            if (atualLimpa.some((numero) => numero !== "")) return atualLimpa;
        }

        const fallbackLimpo = Array.isArray(fallback)
            ? fallback.map((numero) => numero ? String(numero) : "")
            : [...ROTACAO_VAZIA];
        return rotacaoValida(fallbackLimpo) ? fallbackLimpo : [...ROTACAO_VAZIA];
    }

    function copiaRotacaoSegura(rotacao, fallback) {
        return normalizarRotacao(rotacao, fallback, rotacao).map((numero) => String(numero || ""));
    }

    function assinaturaRotacao(rotacao) {
        return Array.isArray(rotacao)
            ? rotacao.map((numero) => String(numero || "")).join("|")
            : "";
    }

    function rotacionarArrayOficial(rotacao) {
        // Ordem visual: [P4, P3, P2, P5, P6, P1].
        if (!rotacaoValida(rotacao)) return rotacao;
        const atual = rotacao.map((numero) => String(numero || ""));
        return [atual[3], atual[0], atual[1], atual[4], atual[5], atual[2]];
    }

    global.VTPRotacaoUtils = Object.freeze({
        numeroAtletaOperacional,
        normalizarAtletaOperacional,
        mesclarAtletasComRotacao,
        montarFallbackRotacaoSeguro,
        rotacaoValida,
        normalizarRotacao,
        copiaRotacaoSegura,
        assinaturaRotacao,
        rotacionarArrayOficial
    });
})(window);
