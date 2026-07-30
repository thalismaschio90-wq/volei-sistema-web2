/* Move o painel Registrar ação para a lateral no desktop,
   deixando botões importantes visíveis sem estragar o mobile/tablet. */
document.addEventListener("DOMContentLoaded", function () {
    try {
        if (!window.matchMedia || !window.matchMedia("(min-width: 1025px) and (hover: hover) and (pointer: fine)").matches) return;
        const lateral = document.querySelector(".jogo-coluna-lateral");
        const acoes = document.querySelector(".jogo-acoes-card");
        const historico = document.querySelector(".historico-card");
        if (lateral && acoes && historico && acoes.parentElement !== lateral) {
            lateral.insertBefore(acoes, historico);
        }
    } catch (e) {
        console.warn("Não foi possível reorganizar o desktop:", e);
    }
});
