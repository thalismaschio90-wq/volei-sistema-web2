(function () {
    "use strict";

    if (window.VolleyWakeLock) {
        window.VolleyWakeLock.ativar("script-recarregado");
        return;
    }

    let sentinel = null;
    let tentando = false;
    let timerRetentativa = null;
    let ultimoErro = "";

    function criarBotao() {
        let botao = document.getElementById("volleyWakeLockButton");
        if (botao) return botao;

        botao = document.createElement("button");
        botao.id = "volleyWakeLockButton";
        botao.type = "button";
        botao.setAttribute("aria-label", "Manter a tela acesa");
        botao.style.cssText = [
            "position:fixed",
            "right:10px",
            "bottom:10px",
            "z-index:2147483647",
            "border:1px solid rgba(255,255,255,.24)",
            "border-radius:999px",
            "padding:9px 12px",
            "font:800 12px/1.1 system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
            "color:#fff",
            "background:rgba(8,25,45,.88)",
            "box-shadow:0 8px 24px rgba(0,0,0,.28)",
            "backdrop-filter:blur(8px)",
            "-webkit-backdrop-filter:blur(8px)",
            "cursor:pointer",
            "touch-action:manipulation"
        ].join(";");
        botao.textContent = "🔒 Manter tela acesa";
        botao.addEventListener("click", function () {
            ativar("botao");
        });
        document.body.appendChild(botao);
        return botao;
    }

    function atualizarBotao(estado) {
        if (!document.body) return;
        const botao = criarBotao();

        if (estado === "ativo") {
            botao.textContent = "✅ Tela ativa";
            botao.style.background = "rgba(16,120,73,.92)";
            botao.title = "A tela permanecerá acesa enquanto este aplicativo estiver visível.";
            return;
        }

        if (estado === "indisponivel") {
            botao.textContent = "⚠️ Ajuste bloqueio automático";
            botao.style.background = "rgba(154,92,0,.94)";
            botao.title = "Este aparelho não liberou o bloqueio de tela. Ajuste temporariamente o bloqueio automático nas configurações do celular.";
            return;
        }

        botao.textContent = "🔒 Manter tela acesa";
        botao.style.background = "rgba(8,25,45,.88)";
        botao.title = ultimoErro || "Toque para impedir que a tela apague.";
    }

    function reagendar() {
        clearTimeout(timerRetentativa);
        timerRetentativa = setTimeout(function () {
            if (document.visibilityState === "visible") ativar("retentativa");
        }, 2500);
    }

    async function ativar(origem) {
        if (tentando || document.visibilityState !== "visible") return false;
        if (!("wakeLock" in navigator) || !navigator.wakeLock || !navigator.wakeLock.request) {
            atualizarBotao("indisponivel");
            return false;
        }
        if (sentinel && !sentinel.released) {
            atualizarBotao("ativo");
            return true;
        }

        tentando = true;
        try {
            sentinel = await navigator.wakeLock.request("screen");
            ultimoErro = "";
            atualizarBotao("ativo");

            sentinel.addEventListener("release", function () {
                sentinel = null;
                atualizarBotao("inativo");
                if (document.visibilityState === "visible") reagendar();
            }, { once: true });
            return true;
        } catch (erro) {
            sentinel = null;
            ultimoErro = erro && erro.message ? String(erro.message) : "Não foi possível manter a tela acesa.";
            atualizarBotao("inativo");
            reagendar();
            return false;
        } finally {
            tentando = false;
        }
    }

    async function liberar() {
        clearTimeout(timerRetentativa);
        if (sentinel && !sentinel.released) {
            try { await sentinel.release(); } catch (e) {}
        }
        sentinel = null;
    }

    function reativar() {
        if (document.visibilityState === "visible") {
            setTimeout(function () { ativar("retorno"); }, 80);
        }
    }

    document.addEventListener("visibilitychange", reativar, { passive: true });
    window.addEventListener("pageshow", reativar, { passive: true });
    window.addEventListener("focus", reativar, { passive: true });
    window.addEventListener("online", reativar, { passive: true });

    // Em alguns aparelhos o primeiro pedido só é aceito após uma interação do usuário.
    ["pointerdown", "touchstart", "click", "keydown"].forEach(function (evento) {
        document.addEventListener(evento, function () {
            ativar("interacao");
        }, { passive: true, capture: true });
    });

    window.addEventListener("beforeunload", liberar);

    window.VolleyWakeLock = {
        ativar: ativar,
        liberar: liberar,
        estaAtivo: function () { return !!(sentinel && !sentinel.released); }
    };

    function iniciar() {
        criarBotao();
        ativar("inicio");
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", iniciar, { once: true });
    } else {
        iniciar();
    }
})();
