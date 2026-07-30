(function (global) {
    "use strict";

    const MAPA_TIPO_SANCAO = Object.freeze({
        advertencia: "advertencia",
        penalidade: "penalidade",
        expulsao: "expulsao",
        desqualificacao: "desqualificacao"
    });

    function criarController(opcoes) {
        const el = opcoes.elementos;
        const estado = {
            sancao: { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "", tipo: "" },
            verde: { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "" },
            retardamento: { equipe: "" }
        };
        const enviando = { sancao: false, verde: false, retardamento: false };

        function opcoesEquipe(baseClass) {
            return [
                { valor: "A", label: opcoes.nomeEquipePorLado("A"), baseClass },
                { valor: "B", label: opcoes.nomeEquipePorLado("B"), baseClass }
            ];
        }

        function opcoesTipoPessoa(baseClass) {
            return [
                { valor: "jogador", label: "Jogador", baseClass },
                { valor: "comissao", label: "Comissão", baseClass }
            ];
        }

        function atualizarResumoSancao() {
            const s = estado.sancao;
            el.sancaoResumo.innerHTML = `Equipe: <strong>${s.equipe || "-"}</strong> &nbsp;•&nbsp; Pessoa: <strong>${s.tipoPessoa || "-"}</strong> &nbsp;•&nbsp; Alvo: <strong>${s.alvoLabel || s.alvo || "-"}</strong> &nbsp;•&nbsp; Tipo: <strong>${s.tipo || "-"}</strong>`;
        }

        function renderSancaoEquipe() {
            const s = estado.sancao;
            opcoes.renderLista(el.sancaoEquipeLista, opcoesEquipe("sancao-card"), s.equipe, "ativo", (valor) => {
                s.equipe = valor;
                s.alvo = "";
                s.alvoNome = "";
                s.alvoLabel = "";
                renderSancaoEquipe();
                renderSancaoAlvo();
                atualizarResumoSancao();
            }, "Sem equipes.");
        }

        function renderSancaoTipoPessoa() {
            const s = estado.sancao;
            opcoes.renderLista(el.sancaoTipoPessoaLista, opcoesTipoPessoa("sancao-card"), s.tipoPessoa, "ativo", (valor) => {
                s.tipoPessoa = valor;
                s.alvo = "";
                s.alvoNome = "";
                s.alvoLabel = "";
                renderSancaoTipoPessoa();
                renderSancaoAlvo();
                atualizarResumoSancao();
            }, "Sem tipos.");
        }

        function renderSancaoAlvo() {
            const s = estado.sancao;
            const alvos = (s.equipe && s.tipoPessoa)
                ? opcoes.membrosEquipePorLado(s.equipe, s.tipoPessoa).map((item) => ({ ...item, baseClass: "sancao-card" }))
                : [];
            opcoes.renderLista(el.sancaoAlvoLista, alvos, s.alvo, "ativo", (valor) => {
                const selecionado = alvos.find((item) => String(item.valor) === String(valor));
                s.alvo = valor;
                s.alvoNome = String(selecionado?.nome || "").trim();
                s.alvoLabel = String(selecionado?.labelCompleto || selecionado?.title || selecionado?.label || valor || "").trim();
                renderSancaoAlvo();
                atualizarResumoSancao();
            }, "Selecione equipe e tipo.");
        }

        function renderSancaoTipo() {
            const s = estado.sancao;
            const tipos = [
                { valor: "advertencia", label: "Advertência", baseClass: "sancao-card" },
                { valor: "penalidade", label: "Penalidade", baseClass: "sancao-card" },
                { valor: "expulsao", label: "Expulsão", baseClass: "sancao-card" },
                { valor: "desqualificacao", label: "Desqualificação", baseClass: "sancao-card" }
            ];
            opcoes.renderLista(el.sancaoTipoLista, tipos, s.tipo, "ativo", (valor) => {
                s.tipo = valor;
                renderSancaoTipo();
                atualizarResumoSancao();
            }, "Sem tipos.");
        }

        function abrirSancao() {
            Object.assign(estado.sancao, { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "", tipo: "" });
            renderSancaoEquipe();
            renderSancaoTipoPessoa();
            renderSancaoAlvo();
            renderSancaoTipo();
            atualizarResumoSancao();
            el.modalSancaoFundo.style.display = "flex";
        }

        function fecharSancao() {
            el.modalSancaoFundo.style.display = "none";
            Object.assign(estado.sancao, { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "", tipo: "" });
            el.sancaoResumo.textContent = "Selecione equipe, pessoa, alvo e sanção.";
        }

        async function registrarSancao() {
            if (opcoes.partidaFinalizada() || enviando.sancao) return false;
            const s = estado.sancao;
            if (!s.equipe || !s.tipoPessoa || !s.alvo || !s.tipo) {
                opcoes.mostrarErro("Selecione equipe, tipo de pessoa, alvo e sanção.");
                return false;
            }
            const tipoBackend = MAPA_TIPO_SANCAO[s.tipo] || s.tipo;
            enviando.sancao = true;
            try {
                return await opcoes.enviarAcao("sancao", {
                    equipe: s.equipe,
                    tipo_pessoa: s.tipoPessoa,
                    alvo: s.alvo,
                    numero: s.tipoPessoa === "jogador" ? s.alvo : "",
                    nome: s.tipoPessoa === "jogador" ? s.alvoNome : (s.alvoNome || s.alvo),
                    tipo_sancao: tipoBackend,
                    sancao: tipoBackend
                }, fecharSancao);
            } finally {
                enviando.sancao = false;
            }
        }

        function atualizarResumoVerde() {
            const v = estado.verde;
            el.verdeResumo.innerHTML = `Equipe: <strong>${v.equipe || "-"}</strong> &nbsp;•&nbsp; Pessoa: <strong>${v.tipoPessoa || "-"}</strong> &nbsp;•&nbsp; Alvo: <strong>${v.alvoLabel || v.alvo || "-"}</strong>`;
        }

        function renderVerdeEquipe() {
            const v = estado.verde;
            opcoes.renderLista(el.verdeEquipeLista, opcoesEquipe("verde-card"), v.equipe, "ativo", (valor) => {
                v.equipe = valor;
                v.alvo = "";
                v.alvoNome = "";
                v.alvoLabel = "";
                renderVerdeEquipe();
                renderVerdeAlvo();
                atualizarResumoVerde();
            }, "Sem equipes.");
        }

        function renderVerdeTipoPessoa() {
            const v = estado.verde;
            opcoes.renderLista(el.verdeTipoPessoaLista, opcoesTipoPessoa("verde-card"), v.tipoPessoa, "ativo", (valor) => {
                v.tipoPessoa = valor;
                v.alvo = "";
                v.alvoNome = "";
                v.alvoLabel = "";
                renderVerdeTipoPessoa();
                renderVerdeAlvo();
                atualizarResumoVerde();
            }, "Sem tipos.");
        }

        function renderVerdeAlvo() {
            const v = estado.verde;
            const alvos = (v.equipe && v.tipoPessoa)
                ? opcoes.membrosEquipePorLado(v.equipe, v.tipoPessoa).map((item) => ({ ...item, baseClass: "verde-card" }))
                : [];
            opcoes.renderLista(el.verdeAlvoLista, alvos, v.alvo, "ativo", (valor) => {
                const selecionado = alvos.find((item) => String(item.valor) === String(valor));
                v.alvo = valor;
                v.alvoNome = String(selecionado?.nome || "").trim();
                v.alvoLabel = String(selecionado?.labelCompleto || selecionado?.title || selecionado?.label || valor || "").trim();
                renderVerdeAlvo();
                atualizarResumoVerde();
            }, "Selecione equipe e tipo.");
        }

        function abrirVerde() {
            Object.assign(estado.verde, { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "" });
            renderVerdeEquipe();
            renderVerdeTipoPessoa();
            renderVerdeAlvo();
            atualizarResumoVerde();
            el.modalVerdeFundo.style.display = "flex";
        }

        function fecharVerde() {
            el.modalVerdeFundo.style.display = "none";
            Object.assign(estado.verde, { equipe: "", tipoPessoa: "", alvo: "", alvoNome: "", alvoLabel: "" });
            el.verdeResumo.textContent = "Selecione equipe e alvo do cartão verde.";
        }

        async function registrarVerde() {
            if (opcoes.partidaFinalizada() || enviando.verde) return false;
            const v = estado.verde;
            if (!v.equipe || !v.tipoPessoa || !v.alvo) {
                opcoes.mostrarErro("Selecione equipe, tipo de pessoa e alvo.");
                return false;
            }
            enviando.verde = true;
            try {
                return await opcoes.enviarAcao("cartao_verde", {
                    equipe: v.equipe,
                    tipo_pessoa: v.tipoPessoa,
                    alvo: v.alvo,
                    numero: v.tipoPessoa === "jogador" ? v.alvo : "",
                    nome: v.tipoPessoa === "jogador" ? v.alvoNome : (v.alvoNome || v.alvo)
                }, fecharVerde);
            } finally {
                enviando.verde = false;
            }
        }

        function renderRetardamentoEquipe() {
            const r = estado.retardamento;
            opcoes.renderLista(el.retardamentoEquipeLista, opcoesEquipe("sancao-card"), r.equipe, "ativo", (valor) => {
                r.equipe = valor;
                el.retardamentoResumo.innerHTML = `Equipe: <strong>${opcoes.nomeEquipePorLado(valor)}</strong>`;
                renderRetardamentoEquipe();
            }, "Sem equipes.");
        }

        function abrirRetardamento() {
            estado.retardamento.equipe = "";
            renderRetardamentoEquipe();
            el.retardamentoResumo.textContent = "Selecione a equipe que cometeu o retardamento.";
            el.modalRetardamentoFundo.style.display = "flex";
        }

        function fecharRetardamento() {
            el.modalRetardamentoFundo.style.display = "none";
            estado.retardamento.equipe = "";
        }

        async function registrarRetardamento() {
            if (opcoes.partidaFinalizada() || !estado.retardamento.equipe || enviando.retardamento) return false;
            opcoes.limparErro();
            enviando.retardamento = true;
            try {
                return await opcoes.enviarAcao("retardamento", { equipe: estado.retardamento.equipe }, fecharRetardamento);
            } finally {
                enviando.retardamento = false;
            }
        }

        return Object.freeze({
            abrirSancao, fecharSancao, registrarSancao,
            abrirVerde, fecharVerde, registrarVerde,
            abrirRetardamento, fecharRetardamento, registrarRetardamento
        });
    }

    global.VTPSancoesController = Object.freeze({ criarController });
})(window);
