(() => {
    let tentativas = 0;
    const maxTentativas = 40;

    const verificarFluxoFinal = () => {
        tentativas += 1;
        try {
            if (typeof abrirObservacoesAutomaticamente !== "function") {
                if (tentativas < maxTentativas) setTimeout(verificarFluxoFinal, 250);
                return;
            }

            const atual = (typeof estadoAtual === "object" && estadoAtual) ? estadoAtual : {};
            const status = String(atual.status_jogo || atual.status || "").trim().toLowerCase();
            const fase = String(atual.fase_partida || "").trim().toLowerCase();
            const textoTela = String(document.body?.innerText || "").toLowerCase();
            const finalNaTela = textoTela.includes("a partida já está finalizada")
                || textoTela.includes("partida finalizada");

            const finalExplicito = atual.partida_finalizada === true
                || atual.encerrado === true
                || atual.fim_jogo === true
                || atual.abrir_observacoes === true
                || status === "finalizada"
                || status === "encerrado"
                || fase === "encerrado"
                || finalNaTela;

            if (finalExplicito) {
                abrirObservacoesAutomaticamente({
                    ...atual,
                    partida_finalizada: true,
                    fim_jogo: true,
                    abrir_observacoes: true,
                    status_jogo: status || "finalizada",
                    fase_partida: fase || "encerrado"
                });
                return;
            }
        } catch (e) {
            console.warn("watchdog fluxo final:", e);
        }

        if (tentativas < maxTentativas) setTimeout(verificarFluxoFinal, 250);
    };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", () => setTimeout(verificarFluxoFinal, 150));
    } else {
        setTimeout(verificarFluxoFinal, 150);
    }
})();
