(function (global) {
    "use strict";

    function renderLista(container, opcoes, selecionado, classeAtiva, onSelect, vazioTexto) {
        container.innerHTML = "";
        if (!opcoes.length) {
            container.innerHTML = `<div style="font-size:13px; color:#6b7c8c;">${vazioTexto}</div>`;
            return;
        }
        opcoes.forEach((opcao) => {
            const botao = document.createElement("button");
            botao.type = "button";
            botao.className = (opcao.baseClass || "substituicao-card")
                + (String(selecionado) === String(opcao.valor) ? ` ${classeAtiva}` : "");
            botao.textContent = opcao.label;
            botao.title = opcao.title || opcao.label;
            botao.addEventListener("click", () => onSelect(opcao.valor));
            container.appendChild(botao);
        });
    }

    function criarController(opcoes) {
        const elementos = opcoes.elementos;
        let equipeAtual = "";
        let numeroSai = "";
        let numeroEntra = "";

        function bloqueiosDoLado(lado) {
            const mapa = opcoes.obterStatusMapa(lado) || {};
            const bloqueios = {};
            Object.keys(mapa).forEach((numero) => {
                if ((mapa[numero] || {}).tipo === "bloqueado_excepcional") bloqueios[numero] = true;
            });
            return bloqueios;
        }

        function opcoesSaida(lado) {
            const rotacao = opcoes.obterRotacao(lado);
            const atletas = opcoes.mesclarAtletasComRotacao(opcoes.obterAtletas(lado), rotacao);
            const emQuadra = Array.from(new Set(
                (rotacao || []).map(opcoes.numeroAtletaOperacional).filter(Boolean)
            ));
            return emQuadra.map((numero) => {
                const atleta = atletas.find((item) => opcoes.numeroAtletaOperacional(item.numero) === numero)
                    || { numero, nome: "Atleta" };
                return {
                    valor: numero,
                    label: numero,
                    title: `${numero || "-"} - ${atleta.nome || "Atleta"}`,
                    baseClass: "substituicao-card"
                };
            });
        }

        function opcoesEntrada(lado) {
            const rotacao = opcoes.obterRotacao(lado);
            const emQuadra = new Set((rotacao || []).map(opcoes.numeroAtletaOperacional).filter(Boolean));
            const bloqueios = bloqueiosDoLado(lado);
            return opcoes.obterAtletas(lado)
                .filter((atleta) => {
                    const numero = opcoes.numeroAtletaOperacional(atleta.numero);
                    return numero
                        && !emQuadra.has(numero)
                        && numero !== numeroSai
                        && !bloqueios[numero];
                })
                .map((atleta) => ({
                    valor: opcoes.numeroAtletaOperacional(atleta.numero),
                    label: opcoes.numeroAtletaOperacional(atleta.numero),
                    title: `${atleta.numero || "-"} - ${atleta.nome || "Atleta"}`,
                    baseClass: "substituicao-card"
                }));
        }

        function atualizarResumo() {
            const motivo = String(elementos.motivoInput?.value || "").trim();
            elementos.resumo.innerHTML = `Sai: <strong>${numeroSai || "-"}</strong> &nbsp;•&nbsp; Entra: <strong>${numeroEntra || "-"}</strong> &nbsp;•&nbsp; Motivo: <strong>${motivo || "-"}</strong>`;
        }

        function renderEntradas() {
            const lista = equipeAtual ? opcoesEntrada(equipeAtual) : [];
            if (numeroEntra && !lista.some((item) => String(item.valor) === String(numeroEntra))) {
                numeroEntra = "";
            }
            renderLista(elementos.entraLista, lista, numeroEntra, "ativo-entra", (valor) => {
                numeroEntra = String(valor);
                renderEntradas();
                atualizarResumo();
            }, "Sem atletas disponíveis.");
        }

        function renderSaidas() {
            const lista = equipeAtual ? opcoesSaida(equipeAtual) : [];
            renderLista(elementos.saiLista, lista, numeroSai, "ativo-sai", (valor) => {
                numeroSai = String(valor);
                if (numeroEntra === numeroSai) numeroEntra = "";
                renderSaidas();
                renderEntradas();
                atualizarResumo();
            }, "Sem atletas em quadra.");
        }

        function abrir(lado) {
            lado = String(lado || "").trim().toUpperCase();
            if ((lado !== "A" && lado !== "B") || opcoes.partidaFinalizada() || opcoes.enviando()) return;
            equipeAtual = lado;
            numeroSai = "";
            numeroEntra = "";
            if (elementos.motivoInput) elementos.motivoInput.value = "";
            elementos.equipeTexto.textContent = `Equipe: ${opcoes.nomeEquipePorLado(lado)}`;
            renderSaidas();
            renderEntradas();
            atualizarResumo();
            elementos.modalFundo.style.display = "flex";
        }

        function fechar() {
            elementos.modalFundo.style.display = "none";
            equipeAtual = "";
            numeroSai = "";
            numeroEntra = "";
            if (elementos.motivoInput) elementos.motivoInput.value = "";
            elementos.resumo.textContent = "Selecione equipe, quem sai e quem entra.";
        }

        async function confirmar() {
            if (confirmando || opcoes.partidaFinalizada() || !equipeAtual) return false;
            const sai = String(numeroSai || "").trim();
            const entra = String(numeroEntra || "").trim();
            const motivo = String(elementos.motivoInput?.value || "").trim();
            if (!sai || !entra) {
                opcoes.mostrarErro("Selecione corretamente quem sai e quem entra.");
                return false;
            }
            confirmando = true;
            try {
                return await opcoes.enviarAcao("substituicao_excepcional", {
                    equipe: equipeAtual,
                    numero_sai: sai,
                    numero_entra: entra,
                    motivo
                }, fechar);
            } finally {
                confirmando = false;
            }
        }

        if (elementos.motivoInput) {
            elementos.motivoInput.addEventListener("input", atualizarResumo);
        }

        return Object.freeze({ abrir, fechar, confirmar, atualizarResumo });
    }

    global.VTPSubstituicaoExcepcional = Object.freeze({ criarController });
})(window);
