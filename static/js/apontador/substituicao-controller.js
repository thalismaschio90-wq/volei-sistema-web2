(function (global) {
    "use strict";

    function substituicoesUsadasDoLado(lado, dados, contexto) {
        const numeroInteiro = contexto.numeroInteiro;
        if (lado === "A") return numeroInteiro(dados?.subs_a ?? contexto.subsAEl?.textContent, 0);
        return numeroInteiro(dados?.subs_b ?? contexto.subsBEl?.textContent, 0);
    }

    function limiteSubstituicoesAtual(dados, contexto) {
        return contexto.numeroInteiro(dados?.limite_substituicoes ?? contexto.subsLimiteEl?.textContent, 6);
    }

    function podePedirSubstituicao(lado, dados, contexto) {
        return !contexto.partidaFinalizada()
            && substituicoesUsadasDoLado(lado, dados, contexto) < limiteSubstituicoesAtual(dados, contexto);
    }

    function dadosSubstituicaoLado(estadoAtual, lado) {
        lado = String(lado || "").toUpperCase();
        const vinculos = estadoAtual.vinculos_substituicao
            || (estadoAtual.vinculos_substituicao = { A: {}, B: {} });
        const finalizados = estadoAtual.substituidos_finalizados
            || (estadoAtual.substituidos_finalizados = { A: [], B: [] });
        if (!vinculos[lado]) vinculos[lado] = {};
        if (!Array.isArray(finalizados[lado])) finalizados[lado] = [];
        const titulares = (lado === "A" ? estadoAtual.titulares_iniciais_a : estadoAtual.titulares_iniciais_b) || [];
        return { vinculos: vinculos[lado], finalizados: finalizados[lado], titulares: titulares.map(String) };
    }

    function validarSubstituicaoRegularLocal(estadoAtual, lado, sai, entra) {
        lado = String(lado || "").toUpperCase();
        sai = String(sai || "");
        entra = String(entra || "");
        const dados = dadosSubstituicaoLado(estadoAtual, lado);
        if (dados.finalizados.includes(sai) || dados.finalizados.includes(entra)) {
            return { ok: false, mensagem: "Essa dupla já completou a substituição neste set. Use substituição excepcional quando a regra permitir." };
        }
        const vincSai = dados.vinculos[sai];
        const vincEntra = dados.vinculos[entra];
        if (vincSai || vincEntra) {
            const vinculo = vincSai || vincEntra;
            if (vinculo.estado !== "reserva_em_quadra" || sai !== String(vinculo.reserva) || entra !== String(vinculo.titular)) {
                return { ok: false, mensagem: `A substituição deve respeitar a dupla: ${vinculo.reserva} só pode sair para o retorno de ${vinculo.titular}.` };
            }
            return { ok: true, retorno: true, vinculo };
        }
        if (!dados.titulares.includes(sai)) {
            return { ok: false, mensagem: "Uma nova substituição regular deve começar com a saída de um titular inicial deste set." };
        }
        if (dados.titulares.includes(entra)) {
            return { ok: false, mensagem: "O atleta que entra deve ser reserva e ficará vinculado somente ao titular substituído." };
        }
        return { ok: true, retorno: false, vinculo: { titular: sai, reserva: entra, estado: "reserva_em_quadra" } };
    }

    function registrarVinculoSubstituicaoLocal(estadoAtual, lado, sai, entra) {
        const dados = dadosSubstituicaoLado(estadoAtual, lado);
        const validacao = validarSubstituicaoRegularLocal(estadoAtual, lado, sai, entra);
        if (!validacao.ok) return validacao;
        const vinculo = validacao.vinculo;
        if (validacao.retorno) {
            vinculo.estado = "finalizado";
            dados.finalizados.push(String(vinculo.titular), String(vinculo.reserva));
            delete dados.vinculos[String(vinculo.titular)];
            delete dados.vinculos[String(vinculo.reserva)];
        } else {
            dados.vinculos[String(vinculo.titular)] = vinculo;
            dados.vinculos[String(vinculo.reserva)] = vinculo;
        }
        return { ok: true };
    }

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
        let equipeAtual = null;
        let confirmando = false;
        let numeroSai = "";
        let numeroEntra = "";

        function opcoesSaida(lado) {
            const rotacao = opcoes.obterRotacao(lado);
            const elenco = opcoes.mesclarAtletasComRotacao(opcoes.obterAtletas(lado), rotacao);
            const emQuadra = new Set((rotacao || []).map(opcoes.numeroAtletaOperacional).filter(Boolean));
            const mapaStatus = opcoes.obterStatusMapa(lado);
            return Array.from(emQuadra)
                .filter((numero) => {
                    const status = mapaStatus[numero] || {};
                    return !(status.tipo === "titular_retorno" || status.substituicao_encerrada || status.tipo === "bloqueado_excepcional");
                })
                .map((numero) => {
                    const atleta = elenco.find((item) => opcoes.numeroAtletaOperacional(item.numero) === numero)
                        || { numero, nome: "Atleta" };
                    return {
                        valor: numero,
                        label: numero,
                        title: `${numero} - ${atleta.nome || "Atleta"}`,
                        baseClass: "substituicao-card"
                    };
                });
        }

        function opcoesEntrada(lado, numeroSaida) {
            const rotacao = opcoes.obterRotacao(lado);
            const elenco = opcoes.mesclarAtletasComRotacao(opcoes.obterAtletas(lado), rotacao);
            const emQuadra = new Set((rotacao || []).map(opcoes.numeroAtletaOperacional).filter(Boolean));
            const mapaStatus = opcoes.obterStatusMapa(lado);
            const statusSai = mapaStatus[String(numeroSaida || "").trim()];

            if (statusSai && statusSai.tipo === "substituto" && statusSai.vinculo) {
                return elenco
                    .filter((atleta) => opcoes.numeroAtletaOperacional(atleta.numero) === String(statusSai.vinculo).trim())
                    .map((atleta) => ({
                        valor: opcoes.numeroAtletaOperacional(atleta.numero),
                        label: opcoes.numeroAtletaOperacional(atleta.numero),
                        title: `${atleta.numero} - ${atleta.nome || "Atleta"}`,
                        baseClass: "substituicao-card"
                    }));
            }

            return elenco
                .filter((atleta) => {
                    const numero = opcoes.numeroAtletaOperacional(atleta.numero);
                    if (!numero || emQuadra.has(numero)) return false;
                    const status = mapaStatus[numero] || {};
                    if (status.tipo === "substituto" || status.tipo === "titular_substituido") return false;
                    if (status.tipo === "vinculo_encerrado" || status.substituicao_encerrada) return false;
                    if (status.tipo === "bloqueado_excepcional") return false;
                    const vinculo = String(status.vinculo || "").trim();
                    return !vinculo || vinculo === String(numeroSaida || "").trim();
                })
                .map((atleta) => ({
                    valor: opcoes.numeroAtletaOperacional(atleta.numero),
                    label: opcoes.numeroAtletaOperacional(atleta.numero),
                    title: `${atleta.numero} - ${atleta.nome || "Atleta"}`,
                    baseClass: "substituicao-card"
                }));
        }

        function atualizarResumo() {
            if (!elementos.resumo) return;
            if (!equipeAtual) {
                elementos.resumo.textContent = "Selecione quem sai e quem entra.";
                return;
            }
            const sai = numeroSai ? `sai #${numeroSai}` : "selecione quem sai";
            const entra = numeroEntra ? `entra #${numeroEntra}` : "selecione quem entra";
            elementos.resumo.textContent = `Equipe ${opcoes.nomeEquipePorLado(equipeAtual)}: ${sai} • ${entra}`;
        }

        function atualizarEntradas() {
            if (!equipeAtual || !numeroSai) {
                renderLista(elementos.entraLista, [], "", "ativo-entra", function () {}, "Nenhuma opção válida para entrada.");
                return;
            }
            const lista = opcoesEntrada(equipeAtual, numeroSai);
            if (numeroEntra && !lista.some((item) => String(item.valor) === String(numeroEntra))) numeroEntra = "";
            renderLista(elementos.entraLista, lista, numeroEntra, "ativo-entra", (valor) => {
                numeroEntra = String(valor);
                atualizarEntradas();
                atualizarResumo();
            }, "Nenhuma opção válida para entrada.");
        }

        function renderSaidas(lado) {
            renderLista(elementos.saiLista, opcoesSaida(lado), numeroSai, "ativo-sai", (valor) => {
                numeroSai = String(valor);
                numeroEntra = "";
                renderSaidas(lado);
                atualizarEntradas();
                atualizarResumo();
            }, "Nenhum atleta disponível em quadra.");
        }

        function abrir(lado) {
            if (opcoes.partidaFinalizada() || opcoes.enviando()) return;
            if (!opcoes.podePedirSubstituicao(lado)) {
                opcoes.mostrarErro("Limite de substituições atingido para esta equipe neste set.");
                opcoes.atualizarTravasOperacionais();
                return;
            }
            equipeAtual = lado;
            numeroSai = "";
            numeroEntra = "";
            elementos.equipeTexto.textContent = `Equipe ${opcoes.nomeEquipePorLado(lado)}`;
            renderSaidas(lado);
            renderLista(elementos.entraLista, [], "", "ativo-entra", function () {}, "Nenhuma opção válida para entrada.");
            atualizarResumo();
            elementos.modalFundo.style.display = "flex";
        }

        function fechar() {
            elementos.modalFundo.style.display = "none";
            equipeAtual = null;
            numeroSai = "";
            numeroEntra = "";
            elementos.saiLista.innerHTML = "";
            elementos.entraLista.innerHTML = "";
            elementos.resumo.textContent = "Selecione quem sai e quem entra.";
        }

        async function confirmar() {
            if (confirmando || opcoes.partidaFinalizada() || !equipeAtual) return false;
            const sai = String(numeroSai || "").trim();
            const entra = String(numeroEntra || "").trim();
            if (!sai || !entra) {
                opcoes.mostrarErro("Selecione corretamente quem sai e quem entra.");
                return false;
            }
            confirmando = true;
            try {
                return await opcoes.enviarAcao("substituicao", {
                    equipe: equipeAtual,
                    numero_sai: sai,
                    numero_entra: entra
                }, fechar);
            } finally {
                confirmando = false;
            }
        }

        return { abrir, fechar, confirmar, atualizarResumo, atualizarEntradas };
    }

    global.VTPSubstituicoes = Object.freeze({
        substituicoesUsadasDoLado,
        limiteSubstituicoesAtual,
        podePedirSubstituicao,
        dadosSubstituicaoLado,
        validarSubstituicaoRegularLocal,
        registrarVinculoSubstituicaoLocal,
        criarController
    });
})(window);
