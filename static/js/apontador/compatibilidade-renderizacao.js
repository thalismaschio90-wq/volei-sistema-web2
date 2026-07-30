(function () {
    const modal = document.getElementById("modal-atalhos-apontador");
    const btnAbrir = document.getElementById("btn-atalhos-apontador") || document.getElementById("btn-abrir-atalhos-apontador");
    const btnFechar = document.getElementById("btn-fechar-atalhos-apontador");
    const btnSalvar = document.getElementById("btn-salvar-atalhos-apontador");
    const btnLimpar = document.getElementById("btn-limpar-atalhos-apontador");

    const atalhosPadrao = {
        ponto_a: "A",
        ponto_b: "L",
        desfazer: "Z",
        tempo_a: "Q",
        tempo_b: "P",
        substituicao_a: "S",
        substituicao_b: "D",
        sancao: "C",
        cartao_verde: "V",
        retardamento: "R",
        sub_excepcional: "X",
        wo_a: "N",
        wo_b: "M",
        fullscreen: "F",
        placar_ao_vivo: "O",
        inverter_lados: "I"
    };

    const chaveLocalAtalhos = "apontador_atalhos_v2";

    function normalizarAtalhos(base) {
        const resultado = Object.assign({}, atalhosPadrao);
        Object.keys(resultado).forEach((acao) => {
            if (base && typeof base[acao] === "string" && base[acao].trim()) {
                resultado[acao] = base[acao].trim().toUpperCase();
            }
        });
        return resultado;
    }

    function carregarAtalhosLocais() {
        try {
            const salvo = JSON.parse(localStorage.getItem(chaveLocalAtalhos) || "{}");
            atalhos = normalizarAtalhos(salvo);
        } catch (e) {
            atalhos = Object.assign({}, atalhosPadrao);
        }
    }

    function salvarAtalhosLocais() {
        try { localStorage.setItem(chaveLocalAtalhos, JSON.stringify(atalhos)); } catch (e) {}
    }

    const mapaBotoes = {
        ponto_a: "#btn-ponto-a",
        ponto_b: "#btn-ponto-b",
        desfazer: "#btn-desfazer",
        tempo_a: "#btn-tempo-a",
        tempo_b: "#btn-tempo-b",
        substituicao_a: "#btn-sub-a",
        substituicao_b: "#btn-sub-b",
        sancao: "#btn-sancao",
        cartao_verde: "#btn-verde",
        retardamento: "#btn-retardamento",
        sub_excepcional: "#btn-sub-excepcional",
        wo_a: "#btn-wo-a",
        wo_b: "#btn-wo-b",
        fullscreen: "#btn-fullscreen",
        placar_ao_vivo: "#btn-abrir-placar",
        inverter_lados: "#btn-inverter-lados"
    };

    let atalhos = Object.assign({}, atalhosPadrao);
    carregarAtalhosLocais();
    let acaoCapturando = null;

    function modalAberto() {
        return modal && modal.classList.contains("aberto");
    }

    function abrirModal() {
        if (!modal) return;
        acaoCapturando = null;
        modal.classList.add("aberto");
        modal.setAttribute("aria-hidden", "false");
        atualizarTela();
    }

    window.abrirModalAtalhosApontador = abrirModal;

    function fecharModal() {
        if (!modal) return;
        acaoCapturando = null;
        modal.classList.remove("aberto");
        modal.setAttribute("aria-hidden", "true");
        document.querySelectorAll(".atalho-item-apontador").forEach((btn) => btn.classList.remove("aguardando"));
        atualizarTela();
    }

    function teclaDoEvento(event) {
        let principal = event.key || "";

        if (principal === " ") principal = "ESPAÇO";
        else if (principal === "ArrowUp") principal = "↑";
        else if (principal === "ArrowDown") principal = "↓";
        else if (principal === "ArrowLeft") principal = "←";
        else if (principal === "ArrowRight") principal = "→";
        else if (principal.length === 1) principal = principal.toUpperCase();
        else principal = principal.toUpperCase();

        const ignorarSozinha = ["SHIFT", "CONTROL", "CTRL", "ALT", "META", "TAB", "CAPSLOCK"];
        if (ignorarSozinha.includes(principal)) return "";

        const partes = [];
        if (event.ctrlKey && principal !== "CONTROL" && principal !== "CTRL") partes.push("CTRL");
        if (event.altKey && principal !== "ALT") partes.push("ALT");
        if (event.shiftKey && principal !== "SHIFT" && principal.length > 1) partes.push("SHIFT");

        partes.push(principal);
        return partes.join("+");
    }

    function alvoDigitavel() {
        const el = document.activeElement;
        if (!el) return false;

        const tag = String(el.tagName || "").toLowerCase();
        if (["input", "textarea", "select"].includes(tag)) return true;
        if (el.isContentEditable) return true;

        return false;
    }

    function atualizarTela() {
        document.querySelectorAll(".atalho-item-apontador").forEach((btn) => {
            const acao = btn.dataset.atalhoAcao;
            const span = btn.querySelector(".atalho-tecla-apontador");
            if (!span) return;

            if (acaoCapturando === acao) {
                span.textContent = "pressione...";
            } else {
                span.textContent = atalhos[acao] || "-";
            }
        });
    }

    function escolherAcao(btn) {
        acaoCapturando = btn.dataset.atalhoAcao;

        document.querySelectorAll(".atalho-item-apontador").forEach((item) => {
            item.classList.toggle("aguardando", item === btn);
        });

        atualizarTela();
    }

    function definirTecla(tecla) {
        if (!acaoCapturando || !tecla) return;

        Object.keys(atalhos).forEach((acao) => {
            if (atalhos[acao] === tecla) atalhos[acao] = "";
        });

        atalhos[acaoCapturando] = tecla;
        acaoCapturando = null;

        document.querySelectorAll(".atalho-item-apontador").forEach((btn) => btn.classList.remove("aguardando"));
        atualizarTela();
    }

    function buscarAcao(tecla) {
        return Object.keys(atalhos).find((acao) => atalhos[acao] === tecla) || "";
    }

    function executarAcao(acao) {
        const seletor = mapaBotoes[acao];
        if (!seletor) return;

        const botao = document.querySelector(seletor);
        if (!botao || botao.disabled) return;

        botao.click();
    }

    async function carregarAtalhos() {
        carregarAtalhosLocais();
        atualizarTela();
        try {
            const resp = await fetch("/apontador/atalhos", {
                method: "GET",
                headers: {"Accept": "application/json"},
                cache: "no-store"
            });
            if (!resp.ok) return;
            const json = await resp.json();
            if (json && json.ok) {
                atalhos = normalizarAtalhos(json.atalhos || atalhos);
                salvarAtalhosLocais();
                atualizarTela();
            }
        } catch (e) {
            console.warn("Usando atalhos locais/padrão:", e);
        }
    }

    async function salvarAtalhos() {
        try {
            const resp = await fetch("/apontador/atalhos/salvar", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                body: JSON.stringify({atalhos})
            });
            const json = await resp.json();

            if (!json || !json.ok) {
                salvarAtalhosLocais();
                fecharModal();
                return;
            }

            salvarAtalhosLocais();
            fecharModal();
        } catch (e) {
            salvarAtalhosLocais();
            fecharModal();
        }
    }

    function limparAtalhos() {
        atalhos = Object.assign({}, atalhosPadrao);
        salvarAtalhosLocais();
        acaoCapturando = null;
        document.querySelectorAll(".atalho-item-apontador").forEach((btn) => btn.classList.remove("aguardando"));
        atualizarTela();
    }

    if (btnAbrir) btnAbrir.addEventListener("click", abrirModal);
    if (btnFechar) btnFechar.addEventListener("click", fecharModal);
    if (btnSalvar) btnSalvar.addEventListener("click", salvarAtalhos);
    if (btnLimpar) btnLimpar.addEventListener("click", limparAtalhos);

    if (modal) {
        modal.addEventListener("click", function (event) {
            if (event.target === modal) fecharModal();
        });
    }

    document.querySelectorAll(".atalho-item-apontador").forEach((btn) => {
        btn.addEventListener("click", function () {
            escolherAcao(btn);
        });
    });

    document.addEventListener("keydown", function (event) {
        const tecla = teclaDoEvento(event);
        if (!tecla) return;

        if (modalAberto() && acaoCapturando) {
            event.preventDefault();
            event.stopPropagation();
            definirTecla(tecla);
            return;
        }

        if (modalAberto()) {
            if (tecla === "ESCAPE") fecharModal();
            return;
        }

        if (alvoDigitavel()) return;

        const acao = buscarAcao(tecla);
        if (!acao) return;

        event.preventDefault();
        event.stopPropagation();
        if (typeof event.stopImmediatePropagation === "function") {
            event.stopImmediatePropagation();
        }

        // IMPORTANTE:
        // O atalho de ponto deve apenas disparar o MESMO botão da tela.
        // Antes, quando a tecla usada era P/E/F/1/Enter, o mesmo keydown
        // continuava propagando para o modal de scout recém-aberto e acabava
        // entrando no fluxo como se fosse marcação simples/automática.
        // Com o clique no próximo tick, o modal abre limpo e o usuário escolhe
        // o scout normalmente.
        setTimeout(function () {
            executarAcao(acao);
        }, 0);
    }, true);

    document.addEventListener("DOMContentLoaded", carregarAtalhos);
    carregarAtalhos();
})();
