"use strict";

(function () {
    window.VTPModulosApontador.validar();

    const BOOT = window.VolleyTableProApontador.getConfig();
    const {
        normalizarNumeroScout: normalizarNumeroScoutLocal,
        numeroVersaoEstado,
        extrairVersaoEstado,
        normalizarDataTempoMs,
        formatarDataHora,
        formatarDuracao,
        limparNomeComparacao,
        numeroInteiro
    } = window.VolleyTableProApontadorEstadoUtils;
    const HTTP = window.VolleyTableProApontadorApi;
    const btnA = document.getElementById("btn-ponto-a");
    const btnB = document.getElementById("btn-ponto-b");
    const erroBox = document.getElementById("erro-jogo");
    const btnDesfazer = document.getElementById("btn-desfazer");
    const btnTempoA = document.getElementById("btn-tempo-a");
    const btnTempoB = document.getElementById("btn-tempo-b");
    const btnSubA = document.getElementById("btn-sub-a");
    const btnSubB = document.getElementById("btn-sub-b");
    const btnSancao = document.getElementById("btn-sancao");
    const btnVerde = document.getElementById("btn-verde");

    const modalSubFundo = document.getElementById("substituicao-modal-fundo");
    const subEquipeTexto = document.getElementById("substituicao-equipe-texto");
    const subSaiLista = document.getElementById("substituicao-sai-lista");
    const subEntraLista = document.getElementById("substituicao-entra-lista");
    const subResumo = document.getElementById("substituicao-resumo");
    const btnSubCancelar = document.getElementById("btn-sub-cancelar");
    const btnSubConfirmar = document.getElementById("btn-sub-confirmar");

    const modalSancaoFundo = document.getElementById("sancao-modal-fundo");
    const sancaoEquipeLista = document.getElementById("sancao-equipe-lista");
    const sancaoTipoPessoaLista = document.getElementById("sancao-tipo-pessoa-lista");
    const sancaoAlvoLista = document.getElementById("sancao-alvo-lista");
    const sancaoTipoLista = document.getElementById("sancao-tipo-lista");
    const sancaoResumo = document.getElementById("sancao-resumo");
    const btnSancaoCancelar = document.getElementById("btn-sancao-cancelar");
    const btnSancaoConfirmar = document.getElementById("btn-sancao-confirmar");

    const modalVerdeFundo = document.getElementById("verde-modal-fundo");
    const verdeEquipeLista = document.getElementById("verde-equipe-lista");
    const verdeTipoPessoaLista = document.getElementById("verde-tipo-pessoa-lista");
    const verdeAlvoLista = document.getElementById("verde-alvo-lista");
    const verdeResumo = document.getElementById("verde-resumo");
    const btnVerdeCancelar = document.getElementById("btn-verde-cancelar");
    const btnVerdeConfirmar = document.getElementById("btn-verde-confirmar");

    const modalPontoFundo = document.getElementById("ponto-modal-fundo");
    const pontoEquipeTexto = document.getElementById("ponto-equipe-texto");
    const pontoFundamentoLista = document.getElementById("ponto-fundamento-lista");
    const pontoResultadoBloco = document.getElementById("ponto-resultado-bloco");
    const pontoResultadoLista = document.getElementById("ponto-resultado-lista");
    const pontoTipoErroBloco = document.getElementById("ponto-tipo-erro-bloco");
    const pontoTipoErroLista = document.getElementById("ponto-tipo-erro-lista");
    const pontoAtletaBloco = document.getElementById("ponto-atleta-bloco");
    const pontoAtletaLabel = document.getElementById("ponto-atleta-label");
    const pontoAtletaLista = document.getElementById("ponto-atleta-lista");
    const pontoResumo = document.getElementById("ponto-resumo");
    const btnPontoCancelar = document.getElementById("btn-ponto-cancelar");
    const btnPontoConfirmar = document.getElementById("btn-ponto-confirmar");
const btnRetardamento = document.getElementById("btn-retardamento");
const btnSubExcepcional = document.getElementById("btn-sub-excepcional");
const btnFullscreen = document.getElementById("btn-fullscreen");
const btnAbrirPlacar = document.getElementById("btn-abrir-placar");
const btnInverterLados = document.getElementById("btn-inverter-lados");
const btnMobileInverterLados = document.getElementById("mobile-inverter-lados");
const btnWoA = document.getElementById("btn-wo-a");
const btnWoB = document.getElementById("btn-wo-b");
const alertaPointEl = document.getElementById("alerta-point");

const popupJogoGrandeEl = document.getElementById("popup-jogo-grande");
const popupJogoFecharEl = document.getElementById("popup-jogo-fechar");
const popupJogoTituloEl = document.getElementById("popup-jogo-titulo");
const popupJogoNumeroEl = document.getElementById("popup-jogo-numero");
const popupJogoNomeEl = document.getElementById("popup-jogo-nome");
const popupJogoEquipeEl = document.getElementById("popup-jogo-equipe");
const popupJogoTempoEl = document.getElementById("popup-jogo-tempo");
let popupSaqueTimer = null;
let ultimoPopupSaqueChave = "";
let ultimoPopupSaqueEm = 0;
let popupSaqueBloqueadoAte = 0;
let sequenciaPontoLocal = 0;
let ultimoSideoutAplicado = "";
let ultimoCliquePontoEm = 0;

const ultimaAcaoEl = document.getElementById("ultima-acao");
const historicoAcoesEl = document.getElementById("historico-acoes");

const modalRetardamentoFundo = document.getElementById("retardamento-modal-fundo");
const retardamentoEquipeLista = document.getElementById("retardamento-equipe-lista");
const retardamentoResumo = document.getElementById("retardamento-resumo");
const btnRetardamentoCancelar = document.getElementById("btn-retardamento-cancelar");
const btnRetardamentoConfirmar = document.getElementById("btn-retardamento-confirmar");

const modalSubExFundo = document.getElementById("subex-modal-fundo");
const subExEquipeTexto = document.getElementById("subex-equipe-texto");
const subExSaiLista = document.getElementById("subex-sai-lista");
const subExEntraLista = document.getElementById("subex-entra-lista");
const subExMotivoInput = document.getElementById("subex-motivo");
const subExResumo = document.getElementById("subex-resumo");
const btnSubExCancelar = document.getElementById("btn-subex-cancelar");
const btnSubExConfirmar = document.getElementById("btn-subex-confirmar");


    const pontosA = document.getElementById("pontos-a");
    const pontosB = document.getElementById("pontos-b");
    const setsA = document.getElementById("sets-a");
    const setsB = document.getElementById("sets-b");
    const setAtual = document.getElementById("set-atual");
    const saqueInfo = document.getElementById("saque-info");
    const tempoRestanteA = document.getElementById("tempo-restante-a");
    const tempoRestanteB = document.getElementById("tempo-restante-b");
    const cronometroEl = document.getElementById("cronometro-tempo");
    const subsAEl = document.getElementById("subs-a");
    const subsBEl = document.getElementById("subs-b");
    const subsLimiteEl = document.getElementById("subs-limite");
    const sancoesAEl = document.getElementById("sancoes-a");
    const sancoesBEl = document.getElementById("sancoes-b");
    const verdesAEl = document.getElementById("verdes-a");
    const verdesBEl = document.getElementById("verdes-b");
    const vermelhosAEl = document.getElementById("vermelhos-a");
    const vermelhosBEl = document.getElementById("vermelhos-b");
    const quadraAEl = document.getElementById("quadra-a");
    const quadraBEl = document.getElementById("quadra-b");
    const tituloQuadraEsquerdaEl = document.getElementById("titulo-quadra-esquerda");
    const tituloQuadraDireitaEl = document.getElementById("titulo-quadra-direita");
    const nomeTopoAEl = document.getElementById("nome-a");
    const nomeTopoBEl = document.getElementById("nome-b");


    const mobileEls = {
        escudoEsq: document.getElementById("mobile-escudo-esq"),
        escudoDir: document.getElementById("mobile-escudo-dir"),
        nomeEsq: document.getElementById("mobile-nome-esq"),
        nomeDir: document.getElementById("mobile-nome-dir"),
        subEsq: document.getElementById("mobile-sub-esq"),
        subDir: document.getElementById("mobile-sub-dir"),
        pontosEsq: document.getElementById("mobile-pontos-esq"),
        pontosDir: document.getElementById("mobile-pontos-dir"),
        setsEsq: document.getElementById("mobile-sets-esq"),
        setsDir: document.getElementById("mobile-sets-dir"),
        setAtual: document.getElementById("mobile-set-atual"),
        saque: document.getElementById("mobile-saque"),
        quadraEsqTitulo: document.getElementById("mobile-quadra-esq-titulo"),
        quadraDirTitulo: document.getElementById("mobile-quadra-dir-titulo"),
        menuEsqTitulo: document.getElementById("mobile-menu-esq-titulo"),
        menuDirTitulo: document.getElementById("mobile-menu-dir-titulo"),
        quadraEsq: document.getElementById("mobile-quadra-esq"),
        quadraDir: document.getElementById("mobile-quadra-dir"),
        ultimaAcao: document.getElementById("mobile-ultima-acao"),
        cronometro: document.getElementById("mobile-cronometro"),
        menuEsq: document.getElementById("mobile-menu-esq"),
        menuDir: document.getElementById("mobile-menu-dir")
    };

    const PARTIDA_ID = BOOT.partidaId;
    const COMPETICAO = BOOT.competicao;
    const OPERADOR_LOGIN = BOOT.operadorLogin;
    const OPERADOR_SESSAO_TOKEN = BOOT.operadorSessaoToken;
    const URL_HEARTBEAT_OPERACAO = BOOT.urls.heartbeat;
    const DISPOSITIVO_OPERACIONAL_ID = (function () {
        const chave = 'vt_operador_dispositivo';
        try {
            let id = localStorage.getItem(chave);
            if (!id) {
                id = 'disp_' + Math.random().toString(36).slice(2) + '_' + Date.now().toString(36);
                localStorage.setItem(chave, id);
            }
            try { document.cookie = chave + '=' + encodeURIComponent(id) + ';path=/;max-age=31536000;SameSite=Lax'; } catch (e) {}
            return id;
        } catch (e) {
            return 'disp_temp_' + Date.now().toString(36);
        }
    })();
    // A/B operacional = lado atual da quadra (pode inverter entre sets).
    // A/B cadastro = confronto original salvo no banco (NUNCA inverte).
    // IMPORTANTE: sets_a/sets_b pertencem ao cadastro; pontos_a/pontos_b pertencem ao operacional atual.
    const NOME_EQUIPE_A = BOOT.equipes.A;
    const NOME_EQUIPE_B = BOOT.equipes.B;
    const NOME_EQUIPE_A_CADASTRO = BOOT.equipes.cadastroA;
    const NOME_EQUIPE_B_CADASTRO = BOOT.equipes.cadastroB;
    const ESCUDO_PADRAO = "/static/img/escudo_padrao.svg";
    let ESCUDO_EQUIPE_A = BOOT.equipes.escudoA;
    let ESCUDO_EQUIPE_B = BOOT.equipes.escudoB;
    const escudoAJogo = document.getElementById("escudo-a-jogo");
    const escudoBJogo = document.getElementById("escudo-b-jogo");
    const CHAVE_INVERSAO_QUADRA = `quadra_invertida_${BOOT.competicaoStorage}_${PARTIDA_ID}`;

    const modoOperacao = String(BOOT.modoOperacao || "simples").toLowerCase().trim();
    console.log("MODO OPERAÇÃO APONTADOR:", modoOperacao);
    const REGRAS_JOGO_INICIAIS = {
        sets_tipo: BOOT.regras.setsTipo,
        pontos_set: BOOT.regras.pontosSet,
        pontos_tiebreak: BOOT.regras.pontosTiebreak,
        diferenca_minima: BOOT.regras.diferencaMinima,
        sets_para_vencer: BOOT.regras.setsParaVencer,
        limite_tempos: BOOT.regras.limiteTempos,
        limite_substituicoes: BOOT.regras.limiteSubstituicoes
    };

    atualizarEscudosDoJogo({
        escudo_a: ESCUDO_EQUIPE_A,
        escudo_b: ESCUDO_EQUIPE_B
    });

    let partidaFinalizada = false;
    let enviando = false;
    let jogoTeveAcaoLocal = false;

    let bloquearSyncAte = 0;
    let placarProtegidoAte = 0;
    let rotacaoProtegidaAte = 0;
    let saqueProtegidoAte = 0;
    let ultimoSaqueLocalForcado = "";
    let ultimaAcaoPontoMobileEm = 0;
    let saqueAtual = BOOT.saqueInicial;

    let equipePontoTemp = "";
    let tipoLancePontoTemp = "";
    let detalheLancePontoTemp = "";
    let atletaNumeroPontoTemp = "";
    let atletaNomePontoTemp = "";
    let atletaLabelPontoTemp = "";

    var ladosInvertidos = localStorage.getItem(CHAVE_INVERSAO_QUADRA) === "1";
    window.ladosInvertidos = ladosInvertidos;

    let statusJogadoresA = BOOT.statusJogadoresA;
    let statusJogadoresB = BOOT.statusJogadoresB;
    let sancoesA = BOOT.sancoesA;
    let sancoesB = BOOT.sancoesB;
    let vermelhosA = BOOT.vermelhosA;
    let vermelhosB = BOOT.vermelhosB;
    let verdesA = BOOT.verdesA;
    let verdesB = BOOT.verdesB;

    // Scout local em tempo real, isolado do fluxo principal do apontador.
    const scoutController = window.VTPScoutController.criarController({
        getAtletas: (lado) => lado === "A" ? atletasA : atletasB,
        numeroInteiro,
        normalizarNumero: normalizarNumeroScoutLocal
    });
    const scoutLocalPorLado = scoutController.estado;

    let syncTimer = null;
    let socketConectado = false;
    const SocketSync = window.VTPApontadorSocketSync;
    const socket = SocketSync
        ? SocketSync.criarSocket(typeof io === "function" ? io : null)
        : null;



    
    function aplicarScoutLocal(equipePontuadora, scout) {
        scoutController.aplicar(equipePontuadora, scout);
    }

    const tempoRealController = window.VTPApontadorRealtimeController.criarController({
        socket,
        socketSync: SocketSync,
        numeroInteiro,
        partidaId: PARTIDA_ID,
        competicao: COMPETICAO,
        operadorLogin: OPERADOR_LOGIN,
        operadorSessaoToken: OPERADOR_SESSAO_TOKEN,
        dispositivoId: DISPOSITIVO_OPERACIONAL_ID,
        equipeA: NOME_EQUIPE_A,
        equipeB: NOME_EQUIPE_B,
        getEstadoAtual: () => estadoAtual,
        getSaqueAtual: () => saqueAtual,
        getRotacoes: () => ({ A: rotA, B: rotB }),
        getInfoSacador: () => {
            try { return infoSacadorDoLado(saqueAtual); } catch (_) { return {}; }
        },
        getEstadoVersao: () => estadoVersaoServidor,
        getScout: () => scoutLocalPorLado,
        getLadosInvertidos: () => ladosInvertidos,
        getTransicaoSetEmAndamento: () => transicaoSetEmAndamento,
        getDisciplina: () => ({ sancoesA, sancoesB, vermelhosA, vermelhosB, verdesA, verdesB })
    });

    const montarPayloadTempoReal = (extra = {}) => tempoRealController.montarPayload(extra);
    const emitirEstadoTempoReal = (extra = {}) => tempoRealController.emitirEstado(extra);
    const receberSolicitacaoTempoReal = (dados) => tempoRealController.receberSolicitacao(dados);
    const ehEcoDoProprioApontador = (dados) => tempoRealController.ehEcoDoProprioApontador(dados);

    if (socket && window.VTPRealtimeHeartbeat) {
        window.VTPRealtimeHeartbeat.iniciar(socket, {
            partidaId: String(PARTIDA_ID),
            competicao: String(COMPETICAO || ""),
            perfil: "apontador",
            clienteId: String(DISPOSITIVO_OPERACIONAL_ID || ""),
            obterVersao: () => Number(estadoVersaoServidor || 0),
            aoTimeout: () => { socketConectado = false; }
        });
    }

    if (socket && window.VTPApontadorSocketSync) {
        window.VTPApontadorSocketSync.registrarHandlersApontador(socket, {
            aoConectar: () => {
                socketConectado = true;
                socket.emit("entrar_partida", {
                    competicao: COMPETICAO,
                    partida_id: PARTIDA_ID,
                    suporta_delta: true,
                    perfil: "apontador"
                });
                setTimeout(() => {
                    if (estadoInicialSincronizado) {
                        emitirEstadoTempoReal({ ultima_acao: estadoAtual?.ultima_acao || "Apontador conectado ao tempo real" });
                    }
                }, 120);
            },
            aoDesconectar: () => {
                socketConectado = false;
                agendarSincronizacao(400);
            },
            aoErroConexao: () => {
                socketConectado = false;
                agendarSincronizacao(500, true);
            },
            aoConfirmarEstadoLocal: (resposta) => {
                if (!resposta || typeof resposta !== "object") return;
                atualizarVersaoEstadoServidor(resposta);
                const houveConflito = !!resposta.snapshot_atrasado || !!resposta.conflito_versao || resposta.ok === false;
                if (!houveConflito) return;
                if (resposta.estado_atual) {
                    const estadoOficial = normalizarRespostaJogo({
                        ...resposta.estado_atual,
                        estado_versao: resposta.estado_versao ?? resposta.estado_atual.estado_versao
                    });
                    atualizarVersaoEstadoServidor(estadoOficial, { permitirMenor: false });
                    aplicarEstado(estadoOficial, { fonte: "socket_conflito_versao", forcarVersao: true });
                    estadoInicialSincronizado = true;
                } else {
                    sincronizarEstadoJogo({ forcarInicial: true }).catch(() => {});
                }
            },
            aoReceberEstado: (dados) => {
                if (!dados || typeof dados !== "object" || ehEcoDoProprioApontador(dados)) return;
                ultimoTokenSync++;
                aplicarEstado(normalizarRespostaJogo(dados), { fonte: "socket" });
            },
            aoReceberDelta: (dados) => {
                if (!dados || typeof dados !== "object" || !clienteDeltaEstado) return;
                if (String(dados.partida_id || "") !== String(PARTIDA_ID || "")) return;
                clienteDeltaEstado.receber(dados);
            },
            aoRecuperarPartida: (dados) => {
                if (!clienteDeltaEstado || !dados || String(dados.partida_id || "") !== String(PARTIDA_ID || "")) return;
                clienteDeltaEstado.receberRecuperacao(dados);
            },
            aoReceberSolicitacao: receberSolicitacaoTempoReal,
            aoExecutarTempo: (dados) => {
                if (!dados || String(dados.status || "iniciado").toLowerCase() === "finalizado") return;
                iniciarCronometro(Number(dados.restante ?? dados.segundos ?? dados.duracao ?? 30), dados.equipe || "");
            },
            aoAtualizarCronometro: (dados) => {
                if (!dados) return;
                if (String(dados.status || "").toLowerCase() === "iniciado") {
                    iniciarCronometro(Number(dados.restante ?? dados.segundos ?? dados.duracao ?? 30), dados.equipe || "");
                }
            }
        });
    }
    let ultimoTokenSync = 0;
    let renderPendente = false;
    let renderAnteriorA = [];
    let renderAnteriorB = [];

    const {
        numeroAtletaOperacional,
        normalizarAtletaOperacional,
        mesclarAtletasComRotacao,
        montarFallbackRotacaoSeguro,
        rotacaoValida,
        normalizarRotacao,
        copiaRotacaoSegura,
        assinaturaRotacao,
        rotacionarArrayOficial
    } = window.VTPRotacaoUtils;

    const atletasAOriginais = BOOT.atletasA;
    const atletasBOriginais = BOOT.atletasB;

    let fallbackRotA = [...BOOT.papeletaA];

    let fallbackRotB = [...BOOT.papeletaB];

    fallbackRotA = montarFallbackRotacaoSeguro(fallbackRotA, atletasAOriginais);
    fallbackRotB = montarFallbackRotacaoSeguro(fallbackRotB, atletasBOriginais);

    const tiposLancePonto = [
        { valor: "ponto", label: "Ponto" },
        { valor: "erro", label: "Erro" },
        { valor: "falta", label: "Falta" }
    ];

    function opcoesDetalhePorTipo() {
        const saque = String(saqueAtual || "").trim().toUpperCase();
        const oponente = ladoOponente(equipePontoTemp);

        if (tipoLancePontoTemp === "ponto") {
            const opcoes = [
                { valor: "ataque", label: "1 Ataque" },
                { valor: "bloqueio", label: "2 Bloqueio" }
            ];
            if (saque && saque === equipePontoTemp) {
                opcoes.push({ valor: "ace", label: "3 Ace" });
            }
            return opcoes;
        }

        if (tipoLancePontoTemp === "erro") {
            const opcoes = [];

            if (saque && saque === oponente) {
                opcoes.push({ valor: "erro_saque", label: "1 Erro de saque" });
                opcoes.push({ valor: "erro_geral", label: "2 Erro geral" });
            } else {
                opcoes.push({ valor: "erro_geral", label: "1 Erro geral" });
                opcoes.push({ valor: "erro_saque", label: "2 Erro de saque" });
            }

            return opcoes;
        }

        if (tipoLancePontoTemp === "falta") {
            return [
                { valor: "rede", label: "1 Rede" },
                { valor: "invasao", label: "2 Invasão" },
                { valor: "rotacao", label: "3 Rotação" },
                { valor: "conducao", label: "4 Condução" },
                { valor: "dois_toques", label: "5 Dois toques" }
            ];
        }

        return [];
    }

    function girarRotacaoLocal(lado) {
        if (typeof rotacionarEquipeLocal === "function") {
            rotacionarEquipeLocal(lado);
        }
    }

    let rotA = normalizarRotacao(
        BOOT.rotacaoA,
        fallbackRotA
    );

    let rotB = normalizarRotacao(
        BOOT.rotacaoB,
        fallbackRotB
    );

    const atletasA = mesclarAtletasComRotacao(atletasAOriginais, rotA);
    const atletasB = mesclarAtletasComRotacao(atletasBOriginais, rotB);

    let estadoAtual = normalizarRespostaJogo(BOOT.estadoInicial);

    // A versão vive durante a sessão aberta do apontador. Não é persistida entre
    // recargas para não reaproveitar uma versão antiga após reinício do servidor.
    let estadoVersaoServidor = extrairVersaoEstado(estadoAtual);

    function atualizarVersaoEstadoServidor(dados, opcoes = {}) {
        const recebida = extrairVersaoEstado(dados);
        if (!recebida) return estadoVersaoServidor;

        const permitirMenor = !!opcoes.permitirMenor;
        if (permitirMenor || recebida >= estadoVersaoServidor) {
            estadoVersaoServidor = recebida;
            if (estadoAtual && typeof estadoAtual === "object") {
                estadoAtual.estado_versao = recebida;
            }
        }
        return estadoVersaoServidor;
    }

    function estadoRecebidoEstaAtrasado(dados, opcoes = {}) {
        const recebida = extrairVersaoEstado(dados);
        if (!recebida || !estadoVersaoServidor) return false;
        if (recebida >= estadoVersaoServidor) return false;

        const origem = String(opcoes.fonte || dados?.origem || dados?.fonte || "").toLowerCase();
        const permiteRegressao = !!dados?.desfazer
            || origem.includes("desfazer")
            || !!dados?.transicao_set
            || !!dados?.fim_set
            || !!dados?.set_finalizado
            || !!dados?.permitir_regressao_estado;

        return !permiteRegressao;
    }

    const agendadorRenderDelta = (window.VTPRealtimeRenderScheduler && typeof window.VTPRealtimeRenderScheduler.create === "function")
        ? window.VTPRealtimeRenderScheduler.create({
            telemetryClientType: "apontador",
            render: (novoEstado) => {
                ultimoTokenSync++;
                aplicarEstado(normalizarRespostaJogo(novoEstado), {
                    fonte: "socket_delta",
                    forcarVersao: true
                });
            }
        })
        : null;

    const clienteDeltaEstado = (window.VTPRealtimeDelta && typeof window.VTPRealtimeDelta.create === "function")
        ? window.VTPRealtimeDelta.create({
            partidaId: String(PARTIDA_ID),
            clientType: "apontador",
            getState: () => estadoAtual || {},
            getVersion: () => estadoVersaoServidor,
            setState: (novoEstado, meta) => {
                estadoAtual = novoEstado || {};
                atualizarVersaoEstadoServidor(estadoAtual, { permitirMenor: false });
            },
            setVersion: (versao) => {
                if (versao > estadoVersaoServidor) estadoVersaoServidor = versao;
            },
            onApplied: (novoEstado, delta) => {
                if (agendadorRenderDelta) {
                    agendadorRenderDelta.schedule(novoEstado, {
                        chaves: Object.keys((delta && delta.patch) || {}),
                        removidas: (delta && delta.chaves_removidas) || []
                    });
                } else {
                    ultimoTokenSync++;
                    aplicarEstado(normalizarRespostaJogo(novoEstado), {
                        fonte: "socket_delta",
                        forcarVersao: true
                    });
                }
            },
            onRecoveryRequired: (info) => {
                if (socket && socket.connected) {
                    socket.emit("recuperar_eventos_partida", { partida_id: String(PARTIDA_ID), ultima_versao: Number(info?.versao_atual || 0) });
                    return true;
                }
                return false;
            },
            onSnapshotRequired: () => {
                sincronizarEstadoJogo({ forcarInicial: true }).catch(() => {});
            }
        })
        : null;

    estadoAtual.pontos_a = numeroInteiro(estadoAtual.pontos_a ?? BOOT.estadoInicial.pontos_a ?? 0, 0);
    estadoAtual.pontos_b = numeroInteiro(estadoAtual.pontos_b ?? BOOT.estadoInicial.pontos_b ?? 0, 0);
    estadoAtual.sets_a = numeroInteiro(estadoAtual.sets_a ?? BOOT.estadoInicial.sets_a ?? 0, 0);
    estadoAtual.sets_b = numeroInteiro(estadoAtual.sets_b ?? BOOT.estadoInicial.sets_b ?? 0, 0);
    estadoAtual.set_atual = numeroInteiro(estadoAtual.set_atual ?? BOOT.estadoInicial.set_atual ?? 1, 1);
    estadoAtual.sets_tipo = estadoAtual.sets_tipo || REGRAS_JOGO_INICIAIS.sets_tipo || "melhor_de_3";
    estadoAtual.pontos_set = numeroInteiro(estadoAtual.pontos_set ?? estadoAtual.ponto_alvo_set ?? estadoAtual.pontos_para_vencer_set ?? REGRAS_JOGO_INICIAIS.pontos_set, 25);
    estadoAtual.ponto_alvo_set = estadoAtual.pontos_set;
    estadoAtual.pontos_para_vencer_set = estadoAtual.pontos_set;
    estadoAtual.pontos_tiebreak = numeroInteiro(estadoAtual.pontos_tiebreak ?? REGRAS_JOGO_INICIAIS.pontos_tiebreak, 15);
    estadoAtual.diferenca_minima = numeroInteiro(estadoAtual.diferenca_minima ?? REGRAS_JOGO_INICIAIS.diferenca_minima, 2);
    if (!estadoAtual.sets_para_vencer || numeroInteiro(estadoAtual.sets_para_vencer, 0) <= 0) {
        const st = String(estadoAtual.sets_tipo || "").toLowerCase();
        estadoAtual.sets_para_vencer = ["set_unico", "único", "unico", "1_set", "melhor_de_1"].includes(st) ? 1 : (st === "melhor_de_5" ? 3 : 2);
    }
    estadoAtual.rotacao_a = rotA;
    estadoAtual.rotacao_b = rotB;
    // Regra oficial carregada no início sempre ganha de snapshots antigos com padrão 2/6.
    estadoAtual.limite_tempos = numeroInteiro(REGRAS_JOGO_INICIAIS.limite_tempos ?? estadoAtual.limite_tempos ?? 2, 2);
    estadoAtual.tempos_por_set = estadoAtual.limite_tempos;
    estadoAtual.limite_substituicoes = numeroInteiro(REGRAS_JOGO_INICIAIS.limite_substituicoes ?? estadoAtual.limite_substituicoes ?? 6, 6);
    estadoAtual.substituicoes_por_set = estadoAtual.limite_substituicoes;
    estadoAtual.titulares_iniciais_a = Array.isArray(estadoAtual.titulares_iniciais_a) && estadoAtual.titulares_iniciais_a.length ? estadoAtual.titulares_iniciais_a.map(String) : [...rotA].map(String);
    estadoAtual.titulares_iniciais_b = Array.isArray(estadoAtual.titulares_iniciais_b) && estadoAtual.titulares_iniciais_b.length ? estadoAtual.titulares_iniciais_b.map(String) : [...rotB].map(String);
    if (!estadoAtual.vinculos_substituicao || typeof estadoAtual.vinculos_substituicao !== "object") estadoAtual.vinculos_substituicao = { A: {}, B: {} };
    if (!estadoAtual.vinculos_substituicao.A) estadoAtual.vinculos_substituicao.A = {};
    if (!estadoAtual.vinculos_substituicao.B) estadoAtual.vinculos_substituicao.B = {};
    if (!estadoAtual.substituidos_finalizados || typeof estadoAtual.substituidos_finalizados !== "object") estadoAtual.substituidos_finalizados = { A: [], B: [] };
    if (!Array.isArray(estadoAtual.substituidos_finalizados.A)) estadoAtual.substituidos_finalizados.A = [];
    if (!Array.isArray(estadoAtual.substituidos_finalizados.B)) estadoAtual.substituidos_finalizados.B = [];
    saqueAtual = ladoPorEquipeOuNome(saqueAtual) || ladoPorEquipeOuNome(estadoAtual.saque_atual) || "";
    estadoAtual.saque_atual = saqueAtual;

    const relogioPartida = window.ApontadorRelogioPartida.criar({
        chaveStorage: `vt_tempo_real_${COMPETICAO}_${PARTIDA_ID}`,
        inicioPartidaReal: estadoAtual.inicio_partida_real || BOOT.inicioPartidaReal,
        fimPartidaReal: estadoAtual.fim_partida_real || BOOT.fimPartidaReal,
        normalizarDataTempoMs,
        formatarDataHora,
        formatarDuracao,
        obterEstado: () => estadoAtual,
        relogioDataEl: document.getElementById("relogio-data-real"),
        relogioHoraEl: document.getElementById("relogio-hora-real"),
        relogioDuracaoEl: document.getElementById("relogio-duracao-real"),
        mobileRelogioEl: document.getElementById("mobile-relogio-jogo-real")
    });
    const garantirInicioTempoRealLocal = relogioPartida.garantirInicio;
    const aplicarTempoRealDoBackend = relogioPartida.aplicarBackend;
    relogioPartida.iniciarAtualizacao();

    function equipeVisual(posicaoVisual) {
        const pos = String(posicaoVisual || "").toLowerCase();
        if (pos === "esquerda") return ladosInvertidos ? "B" : "A";
        if (pos === "direita") return ladosInvertidos ? "A" : "B";
        return String(posicaoVisual || "").toUpperCase() === "B" ? "B" : "A";
    }

    function posicaoVisualDaEquipe(equipe) {
        const lado = String(equipe || "").toUpperCase();
        if (!ladosInvertidos) return lado === "A" ? "esquerda" : "direita";
        return lado === "A" ? "direita" : "esquerda";
    }

    function nomeEquipePorLado(lado) {
        return String(lado || "").toUpperCase() === "A" ? NOME_EQUIPE_A : NOME_EQUIPE_B;
    }

    function ladoCadastroPorLadoOperacional(ladoOperacional) {
        const lado = String(ladoOperacional || "").toUpperCase();
        const nomeOperacional = limparNomeComparacao(nomeEquipePorLado(lado));
        const nomeCadastroA = limparNomeComparacao(NOME_EQUIPE_A_CADASTRO);
        const nomeCadastroB = limparNomeComparacao(NOME_EQUIPE_B_CADASTRO);

        if (nomeOperacional && nomeCadastroA && nomeOperacional === nomeCadastroA) return "A";
        if (nomeOperacional && nomeCadastroB && nomeOperacional === nomeCadastroB) return "B";

        // Fallback seguro para partidas antigas sem equipe_a/equipe_b oficial.
        return lado === "B" ? "B" : "A";
    }

    function valorSetEquipeVisual(dados, ladoOperacional, padrao = 0) {
        const ladoCadastro = ladoCadastroPorLadoOperacional(ladoOperacional);
        const raw = ladoCadastro === "A" ? dados?.sets_a : dados?.sets_b;
        return raw ?? padrao;
    }

    function escudoEquipePorLado(lado) {
        return String(lado || "").toUpperCase() === "A" ? (ESCUDO_EQUIPE_A || ESCUDO_PADRAO) : (ESCUDO_EQUIPE_B || ESCUDO_PADRAO);
    }

    function ladoPorEquipeOuNome(valor) {
        const bruto = String(valor || "").trim();
        const upper = bruto.toUpperCase();

        if (upper === "A" || upper === "B") return upper;

        const normalizado = limparNomeComparacao(bruto);
        const nomeA = limparNomeComparacao(NOME_EQUIPE_A);
        const nomeB = limparNomeComparacao(NOME_EQUIPE_B);

        if (normalizado && nomeA && normalizado === nomeA) return "A";
        if (normalizado && nomeB && normalizado === nomeB) return "B";

        return "";
    }

    function atualizarEscudosVisuais() {
        // Os IDs continuam sendo A/B por compatibilidade, mas no PC eles representam
        // o lado VISUAL da quadra. Quando o apontador inverte os lados, os escudos
        // precisam acompanhar os nomes, placar, botões e rotação que já foram invertidos.
        const equipeEsq = typeof equipeVisual === "function" ? equipeVisual("esquerda") : "A";
        const equipeDir = typeof equipeVisual === "function" ? equipeVisual("direita") : "B";
        const nomeEsq = typeof nomeEquipePorLado === "function" ? nomeEquipePorLado(equipeEsq) : NOME_EQUIPE_A;
        const nomeDir = typeof nomeEquipePorLado === "function" ? nomeEquipePorLado(equipeDir) : NOME_EQUIPE_B;

        if (escudoAJogo) {
            escudoAJogo.src = escudoEquipePorLado(equipeEsq);
            escudoAJogo.alt = `Escudo ${nomeEsq}`;
            escudoAJogo.title = nomeEsq;
        }
        if (escudoBJogo) {
            escudoBJogo.src = escudoEquipePorLado(equipeDir);
            escudoBJogo.alt = `Escudo ${nomeDir}`;
            escudoBJogo.title = nomeDir;
        }

        const dotA = document.getElementById("saque-dot-a");
        const dotB = document.getElementById("saque-dot-b");
        if (dotA) dotA.setAttribute("aria-label", `Saque ${nomeEsq}`);
        if (dotB) dotB.setAttribute("aria-label", `Saque ${nomeDir}`);
    }

    function atualizarEscudosDoJogo(dados) {
        dados = dados || {};
        ESCUDO_EQUIPE_A = dados.escudo_a_operacional || dados.escudo_a || dados.equipe_a_escudo || ESCUDO_EQUIPE_A || ESCUDO_PADRAO;
        ESCUDO_EQUIPE_B = dados.escudo_b_operacional || dados.escudo_b || dados.equipe_b_escudo || ESCUDO_EQUIPE_B || ESCUDO_PADRAO;
        atualizarEscudosVisuais();
        if (typeof renderMobilePainel === "function") {
            try { renderMobilePainel(estadoAtual); } catch (e) {}
        }
    }

    function textoSaqueAtual() {
        const saque = ladoPorEquipeOuNome(saqueAtual);
        if (saque === "A" || saque === "B") {
            return `Saque atual: ${nomeEquipePorLado(saque)}`;
        }
        return "Saque atual ainda não definido";
    }

    function abrirPopupJogoGrande(modo, dados = {}) {
        if (!popupJogoGrandeEl) return;
        const tipo = String(modo || "").toLowerCase();

        if (popupSaqueTimer) {
            clearTimeout(popupSaqueTimer);
            popupSaqueTimer = null;
        }

        if (popupJogoTituloEl) popupJogoTituloEl.textContent = tipo === "tempo" ? "TEMPO" : (dados.titulo || "ORDEM DO SAQUE");
        if (popupJogoNumeroEl) {
            popupJogoNumeroEl.style.display = tipo === "tempo" ? "none" : "block";
            popupJogoNumeroEl.textContent = dados.numero || "?";
        }
        if (popupJogoNomeEl) popupJogoNomeEl.textContent = dados.nome || "";
        if (popupJogoEquipeEl) popupJogoEquipeEl.textContent = dados.equipe || "";
        if (popupJogoTempoEl) {
            popupJogoTempoEl.style.display = tipo === "tempo" ? "block" : "none";
            if (tipo === "tempo") popupJogoTempoEl.textContent = String(dados.segundos ?? dados.restante ?? 30);
        }

        popupJogoGrandeEl.classList.add("aberto");

        if (tipo !== "tempo") {
            popupSaqueTimer = setTimeout(() => {
                popupJogoGrandeEl.classList.remove("aberto");
            }, dados.duracao || 4200);
        }
    }

    function fecharPopupJogoGrande() {
        if (popupSaqueTimer) {
            clearTimeout(popupSaqueTimer);
            popupSaqueTimer = null;
        }
        if (popupJogoGrandeEl) popupJogoGrandeEl.classList.remove("aberto");
    }

    if (popupJogoFecharEl) popupJogoFecharEl.addEventListener("click", fecharPopupJogoGrande);

    function atualizarPopupTempo(restante) {
        if (popupJogoTempoEl) popupJogoTempoEl.textContent = String(Math.max(0, numeroInteiro(restante, 0)));
    }


    function infoSacadorDoLado(lado) {
        const equipe = normalizarEquipe(lado);
        const rotacao = equipe === "A" ? rotA : rotB;
        const numero = atletaDaPosicao(rotacao, 1) || normalizarNumeroScout((rotacao || [])[5] || (rotacao || [])[0] || "");
        const atleta = buscarAtletaPorNumero(equipe, numero);
        return {
            equipe,
            equipeNome: nomeEquipePorLado(equipe),
            numero: numero || "?",
            nome: atleta?.nome ? String(atleta.nome).trim() : ""
        };
    }

    function mostrarPopupNovoSaque(lado, origem = "local") {
        const equipe = ladoPorEquipeOuNome(lado);
        if (equipe !== "A" && equipe !== "B") return;

        const agora = Date.now();
        if (origem !== "local") return;
        if (agora < popupSaqueBloqueadoAte) return;

        const info = infoSacadorDoLado(equipe);
        const chave = `${equipe}:${info.numero || "?"}:${info.nome || ""}`;

        // Trava anti-popup louco: evita o mesmo aviso repetir por socket/autosave/render.
        if (chave === ultimoPopupSaqueChave && (agora - ultimoPopupSaqueEm) < 5500) return;

        ultimoPopupSaqueChave = chave;
        ultimoPopupSaqueEm = agora;
        popupSaqueBloqueadoAte = agora + 1200;

        const nome = info.nome || `Atleta nº ${info.numero}`;
        abrirPopupJogoGrande("saque", {
            numero: info.numero,
            nome: nome,
            equipe: info.equipeNome,
            duracao: 2600
        });
    }

    function valorEquipe(dados, equipe, campoA, campoB, padrao = 0) {
        const raw = String(equipe || "").toUpperCase() === "A" ? dados?.[campoA] : dados?.[campoB];
        return raw ?? padrao;
    }

    function marcarBotaoComEquipe(botao, equipe, texto) {
        if (!botao) return;
        const lado = normalizarEquipe(equipe);

        // Esta é a trava principal do bug: o clique nunca deve depender do id visual
        // btn-ponto-a/btn-ponto-b, porque depois da inversão esses ids continuam iguais.
        // O que vale é data-equipe/data-equipe-atual, atualizado junto com o lado visual.
        botao.dataset.equipe = lado;
        botao.dataset.equipeAtual = lado;
        botao.dataset.nomeEquipe = nomeEquipePorLado(lado);
        botao.textContent = texto;
        botao.setAttribute("aria-label", texto);
        botao.setAttribute("title", texto);
    }

    function equipeDoBotao(botao, fallbackVisual) {
        const data = String(botao?.dataset?.equipe || botao?.dataset?.equipeAtual || "").toUpperCase();
        if (data === "A" || data === "B") return data;
        return equipeVisual(fallbackVisual || "esquerda");
    }

    function atualizarTextoBotoesVisual() {
        const equipeEsq = equipeVisual("esquerda");
        const equipeDir = equipeVisual("direita");
        const nomeEsq = nomeEquipePorLado(equipeEsq);
        const nomeDir = nomeEquipePorLado(equipeDir);

        // Os botões ficam vinculados ao que está VISÍVEL em cada lado da quadra.
        // Assim, ao inverter, nomes, placar, quadrinha, botões e atalhos vão juntos.
        marcarBotaoComEquipe(btnA, equipeEsq, `+ ponto ${nomeEsq}`);
        marcarBotaoComEquipe(btnB, equipeDir, `+ ponto ${nomeDir}`);
        marcarBotaoComEquipe(btnTempoA, equipeEsq, `Tempo ${nomeEsq}`);
        marcarBotaoComEquipe(btnTempoB, equipeDir, `Tempo ${nomeDir}`);
        marcarBotaoComEquipe(btnSubA, equipeEsq, `Substituição ${nomeEsq}`);
        marcarBotaoComEquipe(btnSubB, equipeDir, `Substituição ${nomeDir}`);
        marcarBotaoComEquipe(btnWoA, equipeEsq, `${nomeEsq} perdeu por WO`);
        marcarBotaoComEquipe(btnWoB, equipeDir, `${nomeDir} perdeu por WO`);

        if (btnInverterLados) btnInverterLados.textContent = ladosInvertidos ? "Desfazer inversão dos lados" : "Inverter lados";
        if (btnMobileInverterLados) btnMobileInverterLados.textContent = ladosInvertidos ? "⇄ Desfazer" : "⇄ Inverter";
    }

    function renderPainelVisual(dados = estadoAtual) {
        dados = dados || estadoAtual || {};
        const equipeEsq = equipeVisual("esquerda");
        const equipeDir = equipeVisual("direita");
        const nomeEsq = nomeEquipePorLado(equipeEsq);
        const nomeDir = nomeEquipePorLado(equipeDir);

        if (nomeTopoAEl) nomeTopoAEl.textContent = nomeEsq;
        if (nomeTopoBEl) nomeTopoBEl.textContent = nomeDir;
        if (tituloQuadraEsquerdaEl) tituloQuadraEsquerdaEl.textContent = nomeEsq;
        if (tituloQuadraDireitaEl) tituloQuadraDireitaEl.textContent = nomeDir;
        atualizarEscudosVisuais();
        if (saqueInfo) saqueInfo.textContent = textoSaqueAtual();
        if (pontosA) pontosA.textContent = valorEquipe(dados, equipeEsq, "pontos_a", "pontos_b", 0);
        if (pontosB) pontosB.textContent = valorEquipe(dados, equipeDir, "pontos_a", "pontos_b", 0);
        if (setsA) setsA.textContent = valorSetEquipeVisual(dados, equipeEsq, 0);
        if (setsB) setsB.textContent = valorSetEquipeVisual(dados, equipeDir, 0);
        if (tempoRestanteA) tempoRestanteA.textContent = tempoRestanteDoLado(equipeEsq, dados);
        if (tempoRestanteB) tempoRestanteB.textContent = tempoRestanteDoLado(equipeDir, dados);
        if (subsAEl) subsAEl.textContent = valorEquipe(dados, equipeEsq, "subs_a", "subs_b", 0);
        if (subsBEl) subsBEl.textContent = valorEquipe(dados, equipeDir, "subs_a", "subs_b", 0);

        const sancEsq = valorEquipe(dados, equipeEsq, "sancoes_a", "sancoes_b", []);
        const sancDir = valorEquipe(dados, equipeDir, "sancoes_a", "sancoes_b", []);
        const vermEsq = valorEquipe(dados, equipeEsq, "cartoes_vermelhos_a", "cartoes_vermelhos_b", []);
        const vermDir = valorEquipe(dados, equipeDir, "cartoes_vermelhos_a", "cartoes_vermelhos_b", []);
        const verdEsq = valorEquipe(dados, equipeEsq, "cartoes_verdes_a", "cartoes_verdes_b", []);
        const verdDir = valorEquipe(dados, equipeDir, "cartoes_verdes_a", "cartoes_verdes_b", []);
        if (sancoesAEl) sancoesAEl.textContent = Array.isArray(sancEsq) ? sancEsq.length : (sancEsq || 0);
        if (sancoesBEl) sancoesBEl.textContent = Array.isArray(sancDir) ? sancDir.length : (sancDir || 0);
        if (vermelhosAEl) vermelhosAEl.textContent = Array.isArray(vermEsq) ? vermEsq.length : (vermEsq || 0);
        if (vermelhosBEl) vermelhosBEl.textContent = Array.isArray(vermDir) ? vermDir.length : (vermDir || 0);
        if (verdesAEl) verdesAEl.textContent = Array.isArray(verdEsq) ? verdEsq.length : (verdEsq || 0);
        if (verdesBEl) verdesBEl.textContent = Array.isArray(verdDir) ? verdDir.length : (verdDir || 0);
        atualizarTextoBotoesVisual();
        renderMobilePainel(dados);
    }

    function mobileLadoPorPosicao(posicao) {
        return equipeVisual(posicao === "dir" || posicao === "direita" ? "direita" : "esquerda");
    }

    function buscarAtletaPorNumero(lado, numero) {
        const lista = atletasEquipePorLado(lado) || [];
        const alvo = String(numero || "").trim();
        return lista.find((a) => numeroAtletaOperacional(a.numero) === alvo) || null;
    }

    function nomeCurtoAtleta(lado, numero) {
        const atleta = buscarAtletaPorNumero(lado, numero);
        const nome = String((atleta && atleta.nome) || "").trim();
        if (!nome) return "";
        const partes = nome.split(/\s+/).filter(Boolean);
        return partes.slice(0, 2).join(" ");
    }

    function renderMobilePainel(dados = estadoAtual) {
        if (!mobileEls.nomeEsq) return;
        dados = dados || estadoAtual || {};
        const equipeEsq = equipeVisual("esquerda");
        const equipeDir = equipeVisual("direita");
        const nomeEsq = nomeEquipePorLado(equipeEsq);
        const nomeDir = nomeEquipePorLado(equipeDir);

        if (mobileEls.escudoEsq) {
            mobileEls.escudoEsq.src = escudoEquipePorLado(equipeEsq);
            mobileEls.escudoEsq.alt = `Escudo ${nomeEsq}`;
        }
        if (mobileEls.escudoDir) {
            mobileEls.escudoDir.src = escudoEquipePorLado(equipeDir);
            mobileEls.escudoDir.alt = `Escudo ${nomeDir}`;
        }
        mobileEls.nomeEsq.textContent = nomeEsq;
        mobileEls.nomeDir.textContent = nomeDir;
        mobileEls.quadraEsqTitulo.textContent = nomeEsq;
        mobileEls.quadraDirTitulo.textContent = nomeDir;
        mobileEls.menuEsqTitulo.textContent = nomeEsq;
        mobileEls.menuDirTitulo.textContent = nomeDir;
        mobileEls.pontosEsq.textContent = valorEquipe(dados, equipeEsq, "pontos_a", "pontos_b", 0);
        mobileEls.pontosDir.textContent = valorEquipe(dados, equipeDir, "pontos_a", "pontos_b", 0);
        mobileEls.setsEsq.textContent = valorSetEquipeVisual(dados, equipeEsq, 0);
        mobileEls.setsDir.textContent = valorSetEquipeVisual(dados, equipeDir, 0);
        mobileEls.setAtual.textContent = `${dados.set_atual ?? 1}º SET`;
        mobileEls.saque.textContent = textoSaqueAtual();
        mobileEls.subEsq.textContent = `Tempos ${tempoRestanteDoLado(equipeEsq, dados)} • Subs ${valorEquipe(dados, equipeEsq, "subs_a", "subs_b", 0)}`;
        mobileEls.subDir.textContent = `Tempos ${tempoRestanteDoLado(equipeDir, dados)} • Subs ${valorEquipe(dados, equipeDir, "subs_a", "subs_b", 0)}`;
    }

    function renderMobileQuadraContainer(container, rotacao, lado) {
        if (!container) return;
        container.innerHTML = "";

        // A lista lógica continua sendo [P4, P3, P2, P5, P6, P1].
        // A posição visual agora é feita somente pelo CSS, com quadra esquerda e direita espelhadas.
        // Isso não altera saque, rotação, rally point, substituição nem modais.
        const lista = normalizarRotacao(Array.isArray(rotacao) ? rotacao : [], lado === "A" ? fallbackRotA : fallbackRotB, rotacao || []);
        const posicoesOficiais = [4, 3, 2, 5, 6, 1];

        lista.forEach((numero, idx) => {
            const posicao = posicoesOficiais[idx] || (idx + 1);
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "mobile-pos" + classePosicao(numero, lado);
            btn.dataset.equipe = lado;
            btn.dataset.posicao = String(posicao);
            btn.title = numero ? `P${posicao} • #${numero} ${nomeCurtoAtleta(lado, numero)}` : `P${posicao} vazia`;
            btn.innerHTML = `
                <span class="mobile-pos-numero">${numero || "-"}</span>
                <span class="mobile-pos-nome">${nomeCurtoAtleta(lado, numero) || `P${posicao}`}</span>
            `;
            btn.addEventListener("click", function () {
                abrirModalSubstituicao(lado);
            });
            container.appendChild(btn);
        });
    }

    function renderMobileQuadra() {
        if (!mobileEls.quadraEsq) return;
        const equipeEsq = equipeVisual("esquerda");
        const equipeDir = equipeVisual("direita");
        renderMobileQuadraContainer(mobileEls.quadraEsq, rotacaoPorEquipe(equipeEsq), equipeEsq);
        renderMobileQuadraContainer(mobileEls.quadraDir, rotacaoPorEquipe(equipeDir), equipeDir);
    }

    function atualizarMobileCronometro(texto, ativo = true) {
        if (!mobileEls.cronometro) return;
        mobileEls.cronometro.textContent = texto;
        mobileEls.cronometro.classList.toggle("ativo", !!ativo);
    }

    function fecharMenusMobile() {
        if (mobileEls.menuEsq) mobileEls.menuEsq.classList.remove("aberto");
        if (mobileEls.menuDir) mobileEls.menuDir.classList.remove("aberto");
    }


    const renderUI = window.ApontadorRenderUI.criar({
        ultimaAcaoEl,
        historicoAcoesEl,
        mobileUltimaAcaoEl: mobileEls.ultimaAcao,
        documentRef: document
    });
    const atualizarUltimaAcao = renderUI.atualizarUltimaAcao;
    const renderHistoricoBackend = renderUI.renderHistoricoBackend;
    const alternarTelaCheia = renderUI.alternarTelaCheia;

function abrirPlacarAoVivo() {
    window.open(BOOT.urls.placarAoVivo, "_blank");
}

function avisarPlacarAoVivoSobreInversao() {
    const payload = {
        partida_id: PARTIDA_ID,
        competicao: COMPETICAO,
        apontador: OPERADOR_LOGIN,
        invertido: !!ladosInvertidos,
        lados_invertidos_apontador: !!ladosInvertidos
    };

    // Mesma máquina/navegador: o placar recebe pelo evento storage.
    window.ladosInvertidos = ladosInvertidos;
    localStorage.setItem(CHAVE_INVERSAO_QUADRA, ladosInvertidos ? "1" : "0");
    localStorage.setItem(`placar_inversao_apontador_${PARTIDA_ID}`, JSON.stringify({
        ...payload,
        ts: Date.now()
    }));

    // Outro monitor/celular: o placar recebe pelo Socket.IO.
    if (socket && socketConectado) {
        socket.emit("inversao_lados_apontador", payload);
    }
}

function inverterLadosVisual() {
    ladosInvertidos = !ladosInvertidos;
    renderAnteriorA = [];
    renderAnteriorB = [];
    avisarPlacarAoVivoSobreInversao();
    renderPainelVisual(estadoAtual);
    if (typeof atualizarEscudosVisuais === "function") atualizarEscudosVisuais();
    renderQuadra();
    renderMobileQuadra();
}

    function contextoTempos() {
        return {
            estadoAtual,
            numeroInteiro,
            tempoRestanteA,
            tempoRestanteB,
            partidaFinalizada: () => partidaFinalizada
        };
    }

    function limiteTemposAtual(dados) {
        return window.VTPTempos.limiteTemposAtual(dados, numeroInteiro);
    }

    function temposUsadosDoLado(lado, dados) {
        return window.VTPTempos.temposUsadosDoLado(lado, dados, estadoAtual, numeroInteiro);
    }

    function tempoRestanteDoLado(lado, dados) {
        return window.VTPTempos.tempoRestanteDoLado(lado, dados, contextoTempos());
    }

    function substituicoesUsadasDoLado(lado, dados) {
        return window.VTPSubstituicoes.substituicoesUsadasDoLado(lado, dados, {
            numeroInteiro,
            subsAEl,
            subsBEl
        });
    }

    function limiteSubstituicoesAtual(dados) {
        return window.VTPSubstituicoes.limiteSubstituicoesAtual(dados, {
            numeroInteiro,
            subsLimiteEl
        });
    }

    function podePedirTempo(lado, dados) {
        return window.VTPTempos.podePedirTempo(lado, dados, contextoTempos());
    }

    function podePedirSubstituicao(lado, dados) {
        return window.VTPSubstituicoes.podePedirSubstituicao(lado, dados, {
            partidaFinalizada: () => partidaFinalizada,
            numeroInteiro,
            subsAEl,
            subsBEl,
            subsLimiteEl
        });
    }

    function atualizarTravasOperacionais(dados) {
        const travadoGeral = !!partidaFinalizada || !!enviando;
        const equipeTempoEsq = equipeDoBotao(btnTempoA, "esquerda");
        const equipeTempoDir = equipeDoBotao(btnTempoB, "direita");
        const equipeSubEsq = equipeDoBotao(btnSubA, "esquerda");
        const equipeSubDir = equipeDoBotao(btnSubB, "direita");

        // Mantém o botão clicável quando o contador chega a zero para que o
        // apontador receba a mensagem clara de que a equipe não possui mais tempos.
        if (btnTempoA) {
            const semTempoA = !podePedirTempo(equipeTempoEsq, dados);
            btnTempoA.disabled = travadoGeral;
            btnTempoA.setAttribute("aria-disabled", semTempoA ? "true" : "false");
            btnTempoA.title = semTempoA ? "Esta equipe não possui mais pedidos de tempo neste set." : "";
        }

        if (btnTempoB) {
            const semTempoB = !podePedirTempo(equipeTempoDir, dados);
            btnTempoB.disabled = travadoGeral;
            btnTempoB.setAttribute("aria-disabled", semTempoB ? "true" : "false");
            btnTempoB.title = semTempoB ? "Esta equipe não possui mais pedidos de tempo neste set." : "";
        }

        if (btnSubA) {
            btnSubA.disabled = travadoGeral || !podePedirSubstituicao(equipeSubEsq, dados);
            btnSubA.title = !podePedirSubstituicao(equipeSubEsq, dados) ? "Limite de substituições atingido para esta equipe neste set." : "";
        }

        if (btnSubB) {
            btnSubB.disabled = travadoGeral || !podePedirSubstituicao(equipeSubDir, dados);
            btnSubB.title = !podePedirSubstituicao(equipeSubDir, dados) ? "Limite de substituições atingido para esta equipe neste set." : "";
        }
    }

    function travarBotoes(travar) {
        const travado = !!travar || !!partidaFinalizada;

        if (btnA) btnA.disabled = travado;
        if (btnB) btnB.disabled = travado;
        if (btnDesfazer) btnDesfazer.disabled = !!travar;
        if (btnSancao) btnSancao.disabled = travado;
        if (btnVerde) btnVerde.disabled = travado;
        if (btnSubConfirmar) btnSubConfirmar.disabled = travado;
        if (btnSancaoConfirmar) btnSancaoConfirmar.disabled = travado;
        if (btnVerdeConfirmar) btnVerdeConfirmar.disabled = travado;
        if (btnWoA) btnWoA.disabled = travado;
        if (btnWoB) btnWoB.disabled = travado;

        if (travar) {
            if (btnTempoA) btnTempoA.disabled = true;
            if (btnTempoB) btnTempoB.disabled = true;
            if (btnSubA) btnSubA.disabled = true;
            if (btnSubB) btnSubB.disabled = true;
        } else {
            atualizarTravasOperacionais();
        }
    }

    function mostrarErro(msg) {
        // 🔥 IGNORA erro durante envio (evita falso erro)
        if (enviando) return;

        erroBox.textContent = msg || "Erro ao registrar ação.";
        erroBox.style.display = "block";
    }

    function limparErro() {
        erroBox.textContent = "";
        erroBox.style.display = "none";
    }

    const controladorCronometroTempo = window.VTPTempos.criarControladorCronometro({
        partidaId: PARTIDA_ID,
        competicao: COMPETICAO,
        numeroInteiro,
        normalizarEquipe,
        nomeEquipePorLado,
        cronometroEl,
        atualizarMobileCronometro,
        abrirPopupJogoGrande,
        atualizarPopupTempo,
        fecharPopupJogoGrande,
        obterSocket: () => socket
    });

    function iniciarCronometro(segundos = 30, equipe = "") {
        controladorCronometroTempo.iniciar(segundos, equipe);
    }

    function classePosicao(numero, lado) {
        const chave = String(numero || "").trim();
        const mapa = lado === "A" ? statusJogadoresA : statusJogadoresB;
        const st = mapa[chave];

        if (!st || !st.tipo) return "";
        if (st.tipo === "substituto") return " pos-substituto";
        if (st.tipo === "retorno") return " pos-retorno";
        return "";
    }

    function criarCelulasQuadra(container) {
        container.innerHTML = "";
        const celulas = [];
        const posicoesOficiais = [4, 3, 2, 5, 6, 1];
        for (let i = 0; i < 6; i++) {
            const div = document.createElement("div");
            div.className = "pos";
            div.dataset.posicao = String(posicoesOficiais[i]);
            div.title = `P${posicoesOficiais[i]}`;
            div.textContent = "";
            container.appendChild(div);
            celulas.push(div);
        }
        return celulas;
    }

    const celulasQuadraA = criarCelulasQuadra(quadraAEl);
    const celulasQuadraB = criarCelulasQuadra(quadraBEl);

    function atualizarCelulas(celulas, rotacao, lado, anterior) {
        for (let i = 0; i < 6; i++) {
            const numero = rotacao[i] || "";
            const classeExtra = classePosicao(numero, lado);
            const mudou = String((anterior || [])[i] || "") !== String(numero || "");
            celulas[i].textContent = numero;
            celulas[i].className = "pos" + classeExtra + (mudou ? " pos-atualizando" : "");
            celulas[i].title = numero ? `P${celulas[i].dataset.posicao || ""} • #${numero}` : `P${celulas[i].dataset.posicao || ""} vazia`;
        }
    }

    function limparAnimacaoCelulas() {
        celulasQuadraA.forEach((c) => c.classList.remove("pos-atualizando"));
        celulasQuadraB.forEach((c) => c.classList.remove("pos-atualizando"));
    }

    function rotacaoPorEquipe(equipe) {
        return String(equipe || "").toUpperCase() === "A" ? rotA : rotB;
    }

    function renderQuadra() {
        if (renderPendente) return;
        renderPendente = true;
        requestAnimationFrame(() => {
            const equipeEsq = equipeVisual("esquerda");
            const equipeDir = equipeVisual("direita");
            const rotEsq = rotacaoPorEquipe(equipeEsq);
            const rotDir = rotacaoPorEquipe(equipeDir);
            atualizarCelulas(celulasQuadraA, rotEsq, equipeEsq, renderAnteriorA);
            atualizarCelulas(celulasQuadraB, rotDir, equipeDir, renderAnteriorB);
            renderAnteriorA = [...rotEsq];
            renderAnteriorB = [...rotDir];
            renderMobileQuadra();
            setTimeout(limparAnimacaoCelulas, 130);
            renderPendente = false;
        });
    }

    function atualizarContadoresDisciplinares() {
        // Alguns contadores não existem em todos os layouts/modos de operação.
        // Nunca deixe a ausência de um elemento interromper a abertura do jogo.
        if (sancoesAEl) sancoesAEl.textContent = Array.isArray(sancoesA) ? sancoesA.length : 0;
        if (sancoesBEl) sancoesBEl.textContent = Array.isArray(sancoesB) ? sancoesB.length : 0;
        if (verdesAEl) verdesAEl.textContent = Array.isArray(verdesA) ? verdesA.length : 0;
        if (verdesBEl) verdesBEl.textContent = Array.isArray(verdesB) ? verdesB.length : 0;
    }


    const finalizacaoController = window.VTPFinalizacaoController.criar({
        getEstadoAtual: () => estadoAtual,
        regrasIniciais: REGRAS_JOGO_INICIAIS,
        numeroInteiro,
        setsParaVencerPelasRegras,
        http: HTTP,
        urlEncerrar: URLS_ACAO.encerrar,
        urlObservacoes: BOOT.urls.observacoes,
        chaveOperacaoLocal: `voleitable_operacao_local_${COMPETICAO}_${PARTIDA_ID}`,
        carregarFilaOffline,
        salvarFilaOffline,
        montarEstadoManualParaBanco,
        removerJogoOfflineSeFinalizado,
        mostrarErro
    });

    const estadoConfirmaPartidaFinalizada = finalizacaoController.estadoConfirmaPartidaFinalizada;
    const abrirObservacoesAutomaticamente = finalizacaoController.abrirObservacoesAutomaticamente;

    function mostrarAnimacaoPonto(lado) {
        if (!alertaPointEl) return;

        const nome = nomeEquipePorLado(lado);

        alertaPointEl.textContent = "PONTO - " + nome;
        alertaPointEl.style.display = "block";

        alertaPointEl.style.transform = "scale(1.25)";
        alertaPointEl.style.opacity = "1";

        setTimeout(() => {
            alertaPointEl.style.transform = "scale(1)";
        }, 120);

        setTimeout(() => {
            alertaPointEl.style.opacity = "0";
            setTimeout(() => {
                alertaPointEl.style.display = "none";
                alertaPointEl.style.opacity = "1";
            }, 200);
        }, 1000);
    }

    function setsParaVencerPelasRegras(dados) {
        const setsTipo = String(dados?.sets_tipo ?? "").trim().toLowerCase();

        if (["set_unico", "único", "unico", "1_set", "melhor_de_1"].includes(setsTipo)) {
            return 1;
        }

        if (setsTipo === "melhor_de_5") {
            return 3;
        }

        if (setsTipo === "melhor_de_3") {
            return 2;
        }

        return numeroInteiro(dados?.sets_para_vencer ?? 2, 2);
    }

    function limiteDoSetAtualPelasRegras(dados) {
        const setAtualNumero = numeroInteiro(dados?.set_atual ?? 1, 1);
        const setsTipo = String(dados?.sets_tipo ?? "").trim().toLowerCase();
        const pontosSet = numeroInteiro(dados?.pontos_set ?? 25, 25);
        const pontosTiebreak = numeroInteiro(dados?.pontos_tiebreak ?? 15, 15);

        const ehTiebreak =
            (setsTipo === "melhor_de_3" && setAtualNumero === 3) ||
            (setsTipo === "melhor_de_5" && setAtualNumero === 5);

        return ehTiebreak ? pontosTiebreak : pontosSet;
    }

    function ladoVencedorSetLocal(dados) {
        const pontosAAtual = numeroInteiro(dados?.pontos_a ?? dados?.placar_a ?? 0, 0);
        const pontosBAtual = numeroInteiro(dados?.pontos_b ?? dados?.placar_b ?? 0, 0);
        const limite = limiteDoSetAtualPelasRegras(dados);
        const diferenca = numeroInteiro(dados?.diferenca_minima ?? 2, 2);

        if (pontosAAtual >= limite && (pontosAAtual - pontosBAtual) >= diferenca) {
            return "A";
        }

        if (pontosBAtual >= limite && (pontosBAtual - pontosAAtual) >= diferenca) {
            return "B";
        }

        return "";
    }

    function aplicarFimSetPartidaLocal(ladoVencedor, origem = "local") {
        // TRAVA DE SEGURANÇA (18/06): o navegador NÃO pode mais finalizar set/partida sozinho.
        // Antes esta função somava sets localmente e chamava salvar snapshot com finalizar/pausar,
        // o que quebrava melhor de 3/5 e impedia o redirecionamento automático para papeleta/tie-break.
        // Agora a única fonte da verdade é a resposta oficial do backend em POST /ponto.
        atualizarAvisoPoint(estadoAtual);
        return false;
    }

    function atualizarAvisoPoint(dados) {
        if (!alertaPointEl) return;

        dados = dados || {};

        // Se o set já acabou, não mostra MATCH POINT/SET POINT atrasado.
        if (ladoVencedorSetLocal(dados) || dados?.partida_finalizada) {
            alertaPointEl.style.display = "none";
            alertaPointEl.textContent = "";
            return;
        }

        const pontosAAtual = numeroInteiro(dados?.pontos_a ?? dados?.placar_a ?? 0, 0);
        const pontosBAtual = numeroInteiro(dados?.pontos_b ?? dados?.placar_b ?? 0, 0);
        const setsAAtual = numeroInteiro(dados?.sets_a ?? 0, 0);
        const setsBAtual = numeroInteiro(dados?.sets_b ?? 0, 0);
        const limite = limiteDoSetAtualPelasRegras(dados);
        const diferenca = numeroInteiro(dados?.diferenca_minima ?? 2, 2);
        const setsParaVencer = setsParaVencerPelasRegras(dados);

        let lado = "";

        // Mostra point apenas quando o PRÓXIMO ponto daquele lado encerraria o set.
        // Em empate (20x20, 21x21, 24x24...) não existe set/match point.
        if ((pontosAAtual + 1) >= limite && ((pontosAAtual + 1) - pontosBAtual) >= diferenca && pontosAAtual >= pontosBAtual) {
            lado = "A";
        }

        if ((pontosBAtual + 1) >= limite && ((pontosBAtual + 1) - pontosAAtual) >= diferenca && pontosBAtual >= pontosAAtual) {
            lado = "B";
        }

        if (!lado) {
            alertaPointEl.style.display = "none";
            alertaPointEl.textContent = "";
            return;
        }

        const setsDoLado = lado === "A" ? setsAAtual : setsBAtual;
        const ehMatchPoint = (setsDoLado + 1) >= setsParaVencer;
        const nome = nomeEquipePorLado(lado);

        alertaPointEl.textContent = `${ehMatchPoint ? "MATCH POINT" : "SET POINT"} - ${nome}`;
        alertaPointEl.style.display = "block";
    }



function haSubstituicaoLocalPendente(setNumero) {
    setNumero = numeroInteiro(setNumero, 0);
    return carregarFilaOffline().some(item => {
        const tipo = String(item?.tipo || "").toLowerCase();
        const setItem = numeroInteiro(item?.set_numero ?? item?.payload?.set_numero, 0);
        return setItem === setNumero && (tipo === "substituicao" || tipo === "substituicao_excepcional");
    });
}

    function aplicarEstado(dados, opcoes = {}) {
        if (!dados) return;
        dados = normalizarRespostaJogo(dados);

        if (!opcoes.forcarVersao && estadoRecebidoEstaAtrasado(dados, opcoes)) {
            return;
        }

        atualizarVersaoEstadoServidor(dados);
        aplicarTempoRealDoBackend(dados);
        const agora = Date.now();
        const fonteEstado = String(opcoes.fonte || dados.fonte || dados.origem || "").toLowerCase();
        const ehRespostaOficialPonto = fonteEstado.includes("resposta_ponto_fetch") || fonteEstado.includes("fetch_ponto");
        const ehDesfazer = !!dados.desfazer || fonteEstado.includes("desfazer");

        const atualA = numeroInteiro(estadoAtual?.pontos_a ?? estadoAtual?.placar_a ?? 0, 0);
        const atualB = numeroInteiro(estadoAtual?.pontos_b ?? estadoAtual?.placar_b ?? 0, 0);
        const setAnterior = numeroInteiro(estadoAtual?.set_atual ?? 1, 1);
        const setsAnteriorA = numeroInteiro(estadoAtual?.sets_a ?? 0, 0);
        const setsAnteriorB = numeroInteiro(estadoAtual?.sets_b ?? 0, 0);

        const respostaTemPlacarA = dados.pontos_a !== undefined || dados.placar_a !== undefined;
        const respostaTemPlacarB = dados.pontos_b !== undefined || dados.placar_b !== undefined;
        const respostaTemPlacar = respostaTemPlacarA || respostaTemPlacarB;

        const novoA = respostaTemPlacarA ? numeroInteiro(dados.pontos_a ?? dados.placar_a, atualA) : atualA;
        const novoB = respostaTemPlacarB ? numeroInteiro(dados.pontos_b ?? dados.placar_b, atualB) : atualB;
        const totalAtual = atualA + atualB;
        const totalNovo = novoA + novoB;

        const setNovo = numeroInteiro(dados.set_atual ?? setAnterior, setAnterior);

        // Nunca aceita eco atrasado de um set anterior. No modo local, ao abrir o
        // set seguinte o cache/banco pode ainda estar no set 1/2/3 por alguns segundos.
        // Esse retorno antigo era o responsável por a tela "zerar" no primeiro ponto.
        const fontePodeEstarAtrasada = fonteEstado.includes("socket")
            || fonteEstado.includes("sync")
            || fonteEstado.includes("banco")
            || fonteEstado.includes("poll");
        if (!ehDesfazer && fontePodeEstarAtrasada && setNovo < setAnterior) {
            return;
        }

        const setsNovoA = numeroInteiro(dados.sets_a ?? setsAnteriorA, setsAnteriorA);
        const setsNovoB = numeroInteiro(dados.sets_b ?? setsAnteriorB, setsAnteriorB);
        const mudouSetOuPlacarDeSets = setNovo > setAnterior || setsNovoA > setsAnteriorA || setsNovoB > setsAnteriorB || !!dados.fim_set || !!dados.set_finalizado;

        // Proteção principal: resposta parcial de tempo/substituição/sanção/cartão nunca pode zerar
        // ou diminuir o placar atual. Só aceita queda no placar quando há troca real de set.
        const respostaZeradaOuMenor = respostaTemPlacar && totalAtual > 0 && totalNovo < totalAtual;
        // Desfazer é a única ação normal que DEVE aceitar placar menor.
        // A trava abaixo continua protegendo contra socket/autosave velho zerando a tela.
        const podeAtualizarPlacar = ehDesfazer || !respostaZeradaOuMenor || mudouSetOuPlacarDeSets;

        const pontosAplicadosA = podeAtualizarPlacar ? novoA : atualA;
        const pontosAplicadosB = podeAtualizarPlacar ? novoB : atualB;
        const fezPontoA = podeAtualizarPlacar && novoA > atualA;
        const fezPontoB = podeAtualizarPlacar && novoB > atualB;

        // Enquanto houver substituição local ainda não consolidada no fim do set,
        // uma resposta antiga do banco/socket não pode restaurar a formação inicial.
        const preservarFormacaoLocal = !mudouSetOuPlacarDeSets && haSubstituicaoLocalPendente(setAnterior);
        const formacaoLocalProtegida = preservarFormacaoLocal ? {
            rotacao_a: Array.isArray(rotA) ? [...rotA] : [],
            rotacao_b: Array.isArray(rotB) ? [...rotB] : [],
            subs_a: numeroInteiro(estadoAtual?.subs_a, 0),
            subs_b: numeroInteiro(estadoAtual?.subs_b, 0),
            status_jogadores_a: structuredClone(estadoAtual?.status_jogadores_a || statusJogadoresA || {}),
            status_jogadores_b: structuredClone(estadoAtual?.status_jogadores_b || statusJogadoresB || {})
        } : null;

        estadoAtual = {
            ...(estadoAtual || {}),
            ...dados,
            ...(formacaoLocalProtegida || {}),
            pontos_a: pontosAplicadosA,
            pontos_b: pontosAplicadosB,
            placar_a: pontosAplicadosA,
            placar_b: pontosAplicadosB,
            sets_a: setsNovoA,
            sets_b: setsNovoB,
            set_atual: setNovo,
            estado_versao: numeroVersaoEstado(extrairVersaoEstado(dados) || estadoVersaoServidor)
        };
        atualizarVersaoEstadoServidor(estadoAtual);

        if (podeAtualizarPlacar) {
            if (fezPontoA && typeof animarNumero === "function") animarNumero(posicaoVisualDaEquipe("A") === "esquerda" ? pontosA : pontosB);
            if (fezPontoB && typeof animarNumero === "function") animarNumero(posicaoVisualDaEquipe("B") === "esquerda" ? pontosA : pontosB);
            if (fezPontoA) mostrarAnimacaoPonto("A");
            if (fezPontoB) mostrarAnimacaoPonto("B");

            const veioDePonto = String(dados.tipo_evento || dados.evento || "").toLowerCase().includes("ponto") || dados.equipe_pontuadora !== undefined;
            const rotacaoProtegida = Date.now() < rotacaoProtegidaAte;
            // No desfazer, a rotação correta vem reconstruída pelo servidor depois de remover o evento.
            const podeAceitarRotacaoExterna = ehDesfazer || (!veioDePonto && !rotacaoProtegida);

            // Ponto é decidido localmente pelo apontador. Socket/autosave não pode sobrescrever rotação,
            // senão a quadra fica uma hora travando, outra hora girando duas vezes.
            if (podeAceitarRotacaoExterna && Array.isArray(dados.rotacao_a) && dados.rotacao_a.length === 6) rotA = normalizarRotacao(dados.rotacao_a, fallbackRotA, rotA);
            if (podeAceitarRotacaoExterna && Array.isArray(dados.rotacao_b) && dados.rotacao_b.length === 6) rotB = normalizarRotacao(dados.rotacao_b, fallbackRotB, rotB);
        }

        estadoAtual.rotacao_a = rotA;
        estadoAtual.rotacao_b = rotB;

        // Os escudos pertencem às equipes, não às colunas visuais. Recalcula após
        // qualquer estado ou troca de lado para acompanharem nome, rotação e placar.
        atualizarEscudosDoJogo(estadoAtual);
        if (setAtual) setAtual.textContent = `${estadoAtual.set_atual ?? 1}º SET`;

        if (dados.saque_atual !== undefined && dados.saque_atual !== null) {
            const saqueNovo = ladoPorEquipeOuNome(dados.saque_atual);
            const saqueProtegido = agora < saqueProtegidoAte;

            // No mobile o socket/autosave pode devolver um estado velho logo depois do toque.
            // Enquanto a ação local está protegida, só a resposta oficial do POST /ponto
            // pode trocar o saque. Isso elimina o efeito “vai, volta e vai de novo”.
            if (saqueNovo === "A" || saqueNovo === "B") {
                if (ehDesfazer || !saqueProtegido || ehRespostaOficialPonto || saqueNovo === ultimoSaqueLocalForcado) {
                    saqueAtual = saqueNovo;
                    estadoAtual.saque_atual = saqueAtual;
                } else {
                    estadoAtual.saque_atual = saqueAtual;
                }
            }
        }
        if (saqueInfo) saqueInfo.textContent = textoSaqueAtual();
        if (subsLimiteEl && dados.limite_substituicoes !== undefined && dados.limite_substituicoes !== null) {
            subsLimiteEl.textContent = dados.limite_substituicoes;
        }
        if (preservarFormacaoLocal) {
            statusJogadoresA = estadoAtual.status_jogadores_a || statusJogadoresA;
            statusJogadoresB = estadoAtual.status_jogadores_b || statusJogadoresB;
        } else {
            if (dados.status_jogadores_a !== undefined && dados.status_jogadores_a !== null) statusJogadoresA = dados.status_jogadores_a;
            if (dados.status_jogadores_b !== undefined && dados.status_jogadores_b !== null) statusJogadoresB = dados.status_jogadores_b;
        }
        if (dados.sancoes_a !== undefined && dados.sancoes_a !== null) sancoesA = Array.isArray(dados.sancoes_a) ? dados.sancoes_a : sancoesA;
        if (dados.sancoes_b !== undefined && dados.sancoes_b !== null) sancoesB = Array.isArray(dados.sancoes_b) ? dados.sancoes_b : sancoesB;
        if (dados.cartoes_vermelhos_a !== undefined && dados.cartoes_vermelhos_a !== null) vermelhosA = Array.isArray(dados.cartoes_vermelhos_a) ? dados.cartoes_vermelhos_a : vermelhosA;
        if (dados.cartoes_vermelhos_b !== undefined && dados.cartoes_vermelhos_b !== null) vermelhosB = Array.isArray(dados.cartoes_vermelhos_b) ? dados.cartoes_vermelhos_b : vermelhosB;
        if (dados.cartoes_verdes_a !== undefined && dados.cartoes_verdes_a !== null) verdesA = Array.isArray(dados.cartoes_verdes_a) ? dados.cartoes_verdes_a : verdesA;
        if (dados.cartoes_verdes_b !== undefined && dados.cartoes_verdes_b !== null) verdesB = Array.isArray(dados.cartoes_verdes_b) ? dados.cartoes_verdes_b : verdesB;
        estadoAtual.sancoes_a = sancoesA;
        estadoAtual.sancoes_b = sancoesB;
        estadoAtual.cartoes_vermelhos_a = vermelhosA;
        estadoAtual.cartoes_vermelhos_b = vermelhosB;
        estadoAtual.cartoes_verdes_a = verdesA;
        estadoAtual.cartoes_verdes_b = verdesB;

        atualizarEscudosDoJogo(dados);
        if (dados.ultima_acao !== undefined) atualizarUltimaAcao(dados.ultima_acao || "-");
        if (Array.isArray(dados.historico)) renderHistoricoBackend(dados.historico);
        partidaFinalizada = estadoConfirmaPartidaFinalizada(dados);

        // Não finaliza set/partida apenas pelo cálculo local do navegador.
        // A fonte da verdade é a resposta oficial do backend em /ponto, porque
        // ela conhece sets_max/sets_para_vencer da partida e evita encerrar
        // melhor_de_3/melhor_de_5 no 1º set por estado/cache antigo.
        atualizarAvisoPoint(estadoAtual);
        atualizarContadoresDisciplinares();
        renderPainelVisual(estadoAtual);
        atualizarTravasOperacionais(estadoAtual);
        renderQuadra();

        if (dados.fim_set && !dados.partida_finalizada) {
            transicaoSetEmAndamento = true;
            if (autosaveBancoTimer) { clearTimeout(autosaveBancoTimer); autosaveBancoTimer = null; }
            atualizarUltimaAcao(dados.ultima_acao || "Set finalizado");
            travarBotoes(true);

            // O lote do set é enviado sem bloquear a navegação. Nenhum autosave
            // pode devolver status_operacao para em_andamento neste intervalo.
            sincronizarSetEmSegundoPlano(setAnterior, true).catch(() => {});

            const destinoFluxo = dados.url_redirecionamento
                || dados.url
                || (dados.redirecionar_tiebreak ? BOOT.urls.tiebreak : null)
                || BOOT.urls.papeleta;

            // replace evita voltar pelo histórico para a tela do set encerrado.
            window.setTimeout(() => window.location.replace(destinoFluxo), 120);
            return;
        }
        if (partidaFinalizada && !saqueInfo.textContent.includes("Partida finalizada")) {
            saqueInfo.textContent += " • Partida finalizada";
            travarBotoes(true);
        }

        if (partidaFinalizada) {
            salvarSnapshotLocalAutomatico("partida_finalizada");

            // A própria tela já confirmou que alguém atingiu o número de sets
            // necessário. Não dependemos mais de flags opcionais do payload,
            // porque algumas respostas de ponto trazem apenas sets/status.
            // Sintetizamos as flags finais e iniciamos o único envio definitivo.
            const estadoFinalParaEncerrar = {
                ...estadoAtual,
                ...(dados && typeof dados === "object" ? dados : {}),
                partida_finalizada: true,
                fim_jogo: true,
                encerrado: true,
                abrir_observacoes: true,
                status_jogo: String(dados?.status_jogo || estadoAtual?.status_jogo || "finalizada"),
                fase_partida: String(dados?.fase_partida || estadoAtual?.fase_partida || "encerrado")
            };

            // Libera o ciclo atual de renderização antes do POST final. Isso
            // evita que a resposta do último ponto ainda esteja atualizando a
            // interface quando o navegador inicia o redirecionamento.
            // A resposta oficial do último ponto já foi gravada no banco.
            // O chamador de registrarPonto faz o redirecionamento direto para
            // observações; apenas estados locais/socket usam o POST /encerrar.
            if (String(dados?.fonte || "") !== "resposta_ponto_fetch") {
                window.setTimeout(() => {
                    abrirObservacoesAutomaticamente(estadoFinalParaEncerrar);
                }, 80);
            }
        } else {
            salvarJogoOfflineLocal(true);
            salvarSnapshotLocalAutomatico("estado_aplicado");
        }
    }

    // nomeEquipePorLado centralizada acima.

    function tituloBonitoScout(valor) {
        const texto = String(valor || "").trim();
        if (!texto) return "-";
        return texto.replaceAll("_", " ");
    }

    function normalizarNumeroScout(valor) {
    return String(valor ?? "").trim();
    }

    function normalizarEquipe(lado) {
        return String(lado || "").trim().toUpperCase() === "B" ? "B" : "A";
    }

    function ladoOponente(lado) {
        return normalizarEquipe(lado) === "A" ? "B" : "A";
    }

    function ladoResponsavelScout() {
        if (!equipePontoTemp || !tipoLancePontoTemp) return "";

        const equipePontuou = normalizarEquipe(equipePontoTemp);

        // Regra do scout:
        // - Ponto direto: atleta da equipe que recebeu o ponto.
        // - Erro/Falta: atleta da equipe adversária, pois o erro/falta é creditado a quem cometeu.
        if (tipoLancePontoTemp === "ponto") return equipePontuou;
        if (tipoLancePontoTemp === "erro" || tipoLancePontoTemp === "falta") return ladoOponente(equipePontuou);

        return equipePontuou;
    }

    function atletasEquipePorLado(lado) {
        return lado === "A" ? atletasA : atletasB;
    }

    function statusMapaPorLado(lado) {
        return lado === "A" ? statusJogadoresA : statusJogadoresB;
    }

    function atualizarResumoPonto() {
        const equipeNome = equipePontoTemp ? nomeEquipePorLado(equipePontoTemp) : "-";
        const tipoNome = tipoLancePontoTemp ? tituloBonitoScout(tipoLancePontoTemp) : "-";
        const detalheNome = detalheLancePontoTemp ? tituloBonitoScout(detalheLancePontoTemp) : "-";
        const atletaNome = atletaLabelPontoTemp || "-";

        let html = `Equipe que pontua: <strong>${equipeNome}</strong>`;
        if (tipoLancePontoTemp) html += ` &nbsp;•&nbsp; Tipo: <strong>${tipoNome}</strong>`;
        if (detalheLancePontoTemp) html += ` &nbsp;•&nbsp; Lance: <strong>${detalheNome}</strong>`;
        if (atletaLabelPontoTemp) html += ` &nbsp;•&nbsp; Atleta: <strong>${atletaNome}</strong>`;

        pontoResumo.innerHTML = html;

        if (btnPontoConfirmar) {
            btnPontoConfirmar.disabled = !podeConfirmarPontoAvancado();
        }
    }

    function atletaDaPosicao(rotacao, posicao) {
        if (!Array.isArray(rotacao)) return "";

        const mapa = {
            1: 5,
            2: 2,
            3: 1,
            4: 0,
            5: 3,
            6: 4
        };

        const idx = mapa[posicao];
        if (idx === undefined) return "";
        return normalizarNumeroScout(rotacao[idx] || "");
    }

    function opcoesAtletaScoutPonto() {
        const lado = ladoResponsavelScout();
        if (!lado) return [];

        const atletasQuadra = atletasEmQuadraDoLado(lado);
        if (!atletasQuadra.length) return [];

        if (detalheLancePontoTemp === "ataque") {
            return atletasQuadra;
        }

        if (detalheLancePontoTemp === "bloqueio") {
            const rotacao = lado === "A" ? rotA : rotB;
            const numerosPermitidos = new Set(
                [2, 3, 4]
                    .map((p) => atletaDaPosicao(rotacao, p))
                    .filter(Boolean)
            );

            return atletasQuadra.filter((a) => numerosPermitidos.has(normalizarNumeroScout(a.valor)));
        }

        if (detalheLancePontoTemp === "ace") {
            const rotacao = lado === "A" ? rotA : rotB;
            const sacador = atletaDaPosicao(rotacao, 1);
            if (!sacador) return [];

            return atletasQuadra.filter((a) => normalizarNumeroScout(a.valor) === sacador);
        }

        return [];
    }





    function atletasEmQuadraDoLado(lado) {
        const rotacao = lado === "A" ? rotA : rotB;
        const elenco = atletasEquipePorLado(lado) || [];

        const mapaElenco = new Map();
        elenco.forEach((a) => {
            const numero = normalizarNumeroScout(a.numero);
            if (!numero) return;

            mapaElenco.set(numero, {
                valor: numero,
                label: numero,
                title: `${numero} - ${a.nome || "Atleta"}`,
                nome: String(a.nome || "").trim(),
                labelCompleto: `${numero} - ${a.nome || "Atleta"}`,
                baseClass: "ponto-card"
            });
        });

        const usados = new Set();
        const opcoes = [];

        (rotacao || []).forEach((numeroBruto) => {
            const numero = normalizarNumeroScout(numeroBruto);
            if (!numero || usados.has(numero)) return;

            usados.add(numero);

            if (mapaElenco.has(numero)) {
                opcoes.push(mapaElenco.get(numero));
            } else {
                opcoes.push({
                    valor: numero,
                    label: numero,
                    title: numero,
                    nome: "",
                    labelCompleto: numero,
                    baseClass: "ponto-card"
                });
            }
        });

        return opcoes;
    }

    function pontoExigeDetalhe() {
        return !!tipoLancePontoTemp;
    }

    function pontoExigeAtleta() {
        if (!tipoLancePontoTemp || !detalheLancePontoTemp) return false;

        if (tipoLancePontoTemp === "erro") return false;
        if (tipoLancePontoTemp === "falta") return false;

        return [
            "ataque",
            "bloqueio",
            "ace"
        ].includes(detalheLancePontoTemp);
    }

    function podeConfirmarPontoAvancado() {
        if (!equipePontoTemp) return false;
        if (!tipoLancePontoTemp) return false;
        if (pontoExigeDetalhe() && !detalheLancePontoTemp) return false;
        if (pontoExigeAtleta() && !normalizarNumeroScout(atletaNumeroPontoTemp)) return false;
        return true;
    }

    function renderLista(container, opcoes, selecionado, classeAtiva, onSelect, vazioTexto) {
        container.innerHTML = "";
        if (!opcoes.length) {
            container.innerHTML = `<div style="font-size:13px; color:#6b7c8c;">${vazioTexto}</div>`;
            return;
        }

        opcoes.forEach((opcao) => {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = (opcao.baseClass || "substituicao-card") + (String(selecionado) === String(opcao.valor) ? ` ${classeAtiva}` : "");
            btn.textContent = opcao.label;
            btn.title = opcao.title || opcao.label;
            btn.addEventListener("click", function () {
                onSelect(opcao.valor);
            });
            container.appendChild(btn);
        });
    }

    const substituicaoController = window.VTPSubstituicoes.criarController({
        elementos: {
            modalFundo: modalSubFundo,
            equipeTexto: subEquipeTexto,
            saiLista: subSaiLista,
            entraLista: subEntraLista,
            resumo: subResumo
        },
        obterEstado: () => estadoAtual,
        obterRotacao: (lado) => lado === "A" ? rotA : rotB,
        obterAtletas: atletasEquipePorLado,
        obterStatusMapa: statusMapaPorLado,
        mesclarAtletasComRotacao,
        numeroAtletaOperacional,
        nomeEquipePorLado,
        podePedirSubstituicao,
        partidaFinalizada: () => partidaFinalizada,
        enviando: () => enviando,
        mostrarErro,
        atualizarTravasOperacionais,
        enviarAcao: (tipo, payload, aoSucesso) => enviarAcaoRapida(tipo, payload, aoSucesso)
    });

    function abrirModalSubstituicao(lado) {
        return substituicaoController.abrir(lado);
    }

    function fecharModalSubstituicao() {
        return substituicaoController.fechar();
    }

    function membrosEquipePorLado(lado, tipoPessoa) {
        const rotacao = lado === "A" ? rotA : rotB;
        const atletas = mesclarAtletasComRotacao(atletasEquipePorLado(lado), rotacao).map((a) => {
            const numero = numeroAtletaOperacional(a.numero || a.id || a.nome || "");
            const nome = String(a.nome || "Atleta").trim();
            return {
                valor: numero,
                label: numero || nome,
                title: `${numero || "-"} - ${nome}`,
                nome: nome,
                labelCompleto: numero ? `#${numero} - ${nome}` : nome
            };
        });

        if (tipoPessoa === "jogador") {
            return atletas;
        }

        return [
            { valor: "comissao_tecnico", label: "Técnico", title: "Técnico", nome: "Técnico", labelCompleto: "Técnico" },
            { valor: "comissao_auxiliar", label: "Auxiliar", title: "Auxiliar", nome: "Auxiliar", labelCompleto: "Auxiliar" },
            { valor: "comissao_membro", label: "Membro", title: "Membro autorizado", nome: "Membro", labelCompleto: "Membro autorizado" }
        ];
    }

    const sancoesController = window.VTPSancoesController.criarController({
        elementos: {
            modalSancaoFundo,
            sancaoEquipeLista,
            sancaoTipoPessoaLista,
            sancaoAlvoLista,
            sancaoTipoLista,
            sancaoResumo,
            modalVerdeFundo,
            verdeEquipeLista,
            verdeTipoPessoaLista,
            verdeAlvoLista,
            verdeResumo,
            modalRetardamentoFundo,
            retardamentoEquipeLista,
            retardamentoResumo
        },
        renderLista,
        membrosEquipePorLado,
        nomeEquipePorLado,
        partidaFinalizada: () => partidaFinalizada,
        mostrarErro,
        limparErro,
        enviarAcao: (tipo, payload, aoSucesso) => enviarAcaoRapida(tipo, payload, aoSucesso)
    });

    function abrirModalSancao() { return sancoesController.abrirSancao(); }
    function fecharModalSancao() { return sancoesController.fecharSancao(); }
    function registrarSancao() { return sancoesController.registrarSancao(); }
    function abrirModalVerde() { return sancoesController.abrirVerde(); }
    function fecharModalVerde() { return sancoesController.fecharVerde(); }
    function registrarCartaoVerde() { return sancoesController.registrarVerde(); }
    function abrirModalRetardamento() { return sancoesController.abrirRetardamento(); }
    function fecharModalRetardamento() { return sancoesController.fecharRetardamento(); }
    function registrarRetardamento() { return sancoesController.registrarRetardamento(); }

    function abrirModalPonto(equipe) {
        equipePontoTemp = normalizarEquipe(equipe);
        tipoLancePontoTemp = "";
        detalheLancePontoTemp = "";
        atletaNumeroPontoTemp = "";
        atletaNomePontoTemp = "";
        atletaLabelPontoTemp = "";
        pontoEquipeTexto.textContent = `Equipe que recebeu o ponto: ${nomeEquipePorLado(equipePontoTemp)}`;
        renderFluxoPonto();
        modalPontoFundo.style.display = "flex";
    }

    function fecharModalPonto() {
        modalPontoFundo.style.display = "none";
        equipePontoTemp = "";
        tipoLancePontoTemp = "";
        detalheLancePontoTemp = "";
        atletaNumeroPontoTemp = "";
        atletaNomePontoTemp = "";
        atletaLabelPontoTemp = "";
        pontoResultadoBloco.style.display = "none";
        pontoTipoErroBloco.style.display = "none";
        pontoAtletaBloco.style.display = "none";
        pontoFundamentoLista.innerHTML = "";
        pontoResultadoLista.innerHTML = "";
        pontoTipoErroLista.innerHTML = "";
        pontoAtletaLista.innerHTML = "";
        pontoResumo.textContent = "Escolha primeiro se foi ponto, erro ou falta.";
        if (btnPontoConfirmar) btnPontoConfirmar.disabled = false;
    }

    function renderFundamentosPonto() {
        const opcoes = tiposLancePonto.map((item) => ({
            valor: item.valor,
            label: item.label,
            baseClass: "ponto-card"
        }));

        renderLista(
            pontoFundamentoLista,
            opcoes,
            tipoLancePontoTemp,
            "ativo",
            function (valor) {
                tipoLancePontoTemp = String(valor || "").trim();
                detalheLancePontoTemp = "";
                atletaNumeroPontoTemp = "";
                atletaNomePontoTemp = "";
                atletaLabelPontoTemp = "";
                renderFluxoPonto();
            },
            "Nenhuma opção disponível."
        );
    }

    function renderDetalhesPonto() {
        const opcoes = opcoesDetalhePorTipo().map((item) => ({
            valor: item.valor,
            label: item.label,
            baseClass: "ponto-card"
        }));

        pontoResultadoBloco.style.display = tipoLancePontoTemp ? "block" : "none";

        renderLista(
            pontoResultadoLista,
            opcoes,
            detalheLancePontoTemp,
            "ativo",
            function (valor) {
                detalheLancePontoTemp = String(valor || "").trim();
                atletaNumeroPontoTemp = "";
                atletaNomePontoTemp = "";
                atletaLabelPontoTemp = "";
                renderFluxoPonto();
            },
            "Selecione primeiro o tipo."
        );
    }

    function renderAtletasPonto() {
        const exige = pontoExigeAtleta();

        pontoAtletaBloco.style.display = exige ? "block" : "none";
        pontoTipoErroBloco.style.display = "none";

        const equipeResponsavel = ladoResponsavelScout();
        const nomeResponsavel = equipeResponsavel ? nomeEquipePorLado(equipeResponsavel) : "";
        pontoAtletaLabel.textContent = nomeResponsavel
            ? `4. Quem foi responsável no scout? (${nomeResponsavel})`
            : "4. Quem foi responsável no scout?";

        if (!exige) {
            atletaNumeroPontoTemp = "";
            atletaNomePontoTemp = "";
            atletaLabelPontoTemp = "";
            pontoAtletaLista.innerHTML = "";
            atualizarResumoPonto();
            return;
        }

        const opcoes = opcoesAtletaScoutPonto();

        if (
            atletaNumeroPontoTemp &&
            !opcoes.some((o) => normalizarNumeroScout(o.valor) === normalizarNumeroScout(atletaNumeroPontoTemp))
        ) {
            atletaNumeroPontoTemp = "";
            atletaNomePontoTemp = "";
            atletaLabelPontoTemp = "";
        }

        renderLista(
            pontoAtletaLista,
            opcoes,
            atletaNumeroPontoTemp,
            "ativo",
            function (valor) {
                const selecionado = opcoes.find((o) => normalizarNumeroScout(o.valor) === normalizarNumeroScout(valor));
                atletaNumeroPontoTemp = normalizarNumeroScout(valor);
                atletaNomePontoTemp = String((selecionado && selecionado.nome) || "").trim();
                atletaLabelPontoTemp = String(
                    (selecionado && (selecionado.labelCompleto || selecionado.title || selecionado.label)) || valor || ""
                ).trim();
                atualizarResumoPonto();
            },
            "Nenhum atleta disponível para este lance."
        );

        atualizarResumoPonto();
    }

    function renderFluxoPonto() {
        renderFundamentosPonto();
        renderDetalhesPonto();
        renderAtletasPonto();
        atualizarResumoPonto();
    }

    function normalizarRespostaJogo(dados) {
        if (!dados || typeof dados !== "object") return {};

        if (dados.estado && typeof dados.estado === "object") {
            dados = {
                ...dados.estado,
                ok: dados.ok,
                mensagem: dados.mensagem || dados.estado.mensagem || "",
                evento: dados.evento,
                set_finalizado: dados.set_finalizado,
                redirecionar_papeleta: dados.redirecionar_papeleta,
                redirecionar_tiebreak: dados.redirecionar_tiebreak,
                partida_finalizada: dados.partida_finalizada,
                abrir_observacoes: dados.abrir_observacoes,
                encerrado: dados.encerrado,
                url: dados.url,
                url_redirecionamento: dados.url_redirecionamento
            };
        }

        const saida = { ...dados };

        // NÃO inventa placar 0 quando a resposta é só confirmação de tempo/sanção/cartão.
        // Antes isso fazia a tela piscar/zerar e depois voltar no próximo sync.
        const temPlacarA = dados.pontos_a !== undefined || dados.placar_a !== undefined;
        const temPlacarB = dados.pontos_b !== undefined || dados.placar_b !== undefined;
        if (temPlacarA) {
            const pontosA = dados.pontos_a ?? dados.placar_a;
            saida.pontos_a = pontosA;
            saida.placar_a = pontosA;
        }
        if (temPlacarB) {
            const pontosB = dados.pontos_b ?? dados.placar_b;
            saida.pontos_b = pontosB;
            saida.placar_b = pontosB;
        }

        if (dados.rotacao_a !== undefined || (dados.rotacao && dados.rotacao.equipe_a !== undefined)) {
            saida.rotacao_a = dados.rotacao_a ?? dados.rotacao.equipe_a;
        }
        if (dados.rotacao_b !== undefined || (dados.rotacao && dados.rotacao.equipe_b !== undefined)) {
            saida.rotacao_b = dados.rotacao_b ?? dados.rotacao.equipe_b;
        }

        if (Array.isArray(dados.historico)) saida.historico = dados.historico.slice(0, 5);
        if (dados.ultima_acao === undefined && dados.mensagem) saida.ultima_acao = dados.mensagem;

        // Mantém as regras oficiais da competição mesmo quando chega payload parcial
        // de socket/autosave. Sem isso, o cache voltava para 25 e sumia o set/match point em jogos a 21.
        saida.sets_tipo = saida.sets_tipo || (estadoAtual && estadoAtual.sets_tipo) || REGRAS_JOGO_INICIAIS.sets_tipo || "melhor_de_3";
        saida.pontos_set = numeroInteiro(saida.pontos_set ?? saida.ponto_alvo_set ?? saida.pontos_para_vencer_set ?? (estadoAtual && estadoAtual.pontos_set) ?? REGRAS_JOGO_INICIAIS.pontos_set, 25);
        saida.ponto_alvo_set = saida.pontos_set;
        saida.pontos_para_vencer_set = saida.pontos_set;
        saida.pontos_tiebreak = numeroInteiro(saida.pontos_tiebreak ?? (estadoAtual && estadoAtual.pontos_tiebreak) ?? REGRAS_JOGO_INICIAIS.pontos_tiebreak, 15);
        saida.diferenca_minima = numeroInteiro(saida.diferenca_minima ?? (estadoAtual && estadoAtual.diferenca_minima) ?? REGRAS_JOGO_INICIAIS.diferenca_minima, 2);
        saida.sets_para_vencer = numeroInteiro(saida.sets_para_vencer ?? (estadoAtual && estadoAtual.sets_para_vencer) ?? REGRAS_JOGO_INICIAIS.sets_para_vencer, 0);
        if (!saida.sets_para_vencer || saida.sets_para_vencer <= 0) {
            const st = String(saida.sets_tipo || "").toLowerCase();
            saida.sets_para_vencer = ["set_unico", "único", "unico", "1_set", "melhor_de_1"].includes(st) ? 1 : (st === "melhor_de_5" ? 3 : 2);
        }

        return saida;
    }

    function agendarSincronizacao(delay = 250, forcar = false) {
        if (!forcar && socketConectado) return;

        if (syncTimer) clearTimeout(syncTimer);

        syncTimer = setTimeout(() => {
            sincronizarEstadoJogo();
        }, delay);
    }


    async function sincronizarEstadoJogo(opcoes = {}) {
    // Não fica puxando banco durante a operação, para não causar pisca/volta de rotação.
    // Mas, ao abrir em outro computador/celular, precisamos hidratar a tela uma vez
    // com o snapshot salvo no banco antes de o usuário começar a mexer.
    const forcarInicial = !!opcoes.forcarInicial;
    if (jogoTeveAcaoLocal && !forcarInicial) return null;
    if (!URLS_ACAO || !URLS_ACAO.estado) return null;

    try {
        const separadorEstado = URLS_ACAO.estado.includes("?") ? "&" : "?";
        const urlEstadoConsulta = forcarInicial
            ? URLS_ACAO.estado
            : `${URLS_ACAO.estado}${separadorEstado}leve=1`;

        const { resposta: resp, dados } = await HTTP.requisitarJson(urlEstadoConsulta, {
            method: "GET",
            headers: {
                "X-Requested-With": "fetch",
                "Cache-Control": "no-cache"
            },
            cache: "no-store"
        });
        if (!resp.ok || !dados || dados.ok === false) return null;

        // Se o apontador já mexeu localmente, não sobrescreve a operação dele.
        if (jogoTeveAcaoLocal && !forcarInicial) return null;

        aplicarEstado(normalizarRespostaJogo(dados), { fonte: forcarInicial ? "abertura_banco" : "sync_banco" });
        if (forcarInicial) {
            estadoInicialSincronizado = true;
            salvarSnapshotLocalAutomatico("abertura_confirmada");
            if (socketConectado) {
                emitirEstadoTempoReal({ ultima_acao: estadoAtual?.ultima_acao || "Estado oficial carregado" });
            }
        }
        return dados;
    } catch (e) {
        return null;
    }
}


function rotacionarEquipeLocal(lado) {
    lado = String(lado || "").toUpperCase();
    if (lado === "A") {
        rotA = rotacionarArrayOficial(copiaRotacaoSegura(rotA, fallbackRotA));
        estadoAtual.rotacao_a = [...rotA];
        return true;
    }
    if (lado === "B") {
        rotB = rotacionarArrayOficial(copiaRotacaoSegura(rotB, fallbackRotB));
        estadoAtual.rotacao_b = [...rotB];
        return true;
    }
    return false;
}

function aplicarRegraSaqueERotacao(equipePontuadora) {
    const equipe = ladoPorEquipeOuNome(equipePontuadora);
    if (equipe !== "A" && equipe !== "B") return { girou: false, saqueAnterior: "", saqueNovo: "" };

    const saqueAnterior = ladoPorEquipeOuNome(saqueAtual || estadoAtual?.saque_atual || "");
    const assinaturaAntes = `${sequenciaPontoLocal}:${equipe}:${saqueAnterior || "sem-saque"}:${assinaturaRotacao(rotA)}:${assinaturaRotacao(rotB)}`;

    // Sem saque definido: apenas define o primeiro saque. Não roda.
    if (!saqueAnterior) {
        atualizarSaqueLocal(equipe, true);
        return { girou: false, saqueAnterior: "", saqueNovo: equipe };
    }

    // Mesma equipe manteve o saque: ponto normal, sem rotação.
    if (saqueAnterior === equipe) {
        atualizarSaqueLocal(equipe, false);
        return { girou: false, saqueAnterior, saqueNovo: equipe };
    }

    // Side-out: a equipe que ganhou o ponto recuperou o saque. Roda exatamente uma vez.
    if (ultimoSideoutAplicado !== assinaturaAntes) {
        rotacionarEquipeLocal(equipe);
        ultimoSideoutAplicado = assinaturaAntes;
    }

    atualizarSaqueLocal(equipe, true);
    return { girou: true, saqueAnterior, saqueNovo: equipe };
}

function atualizarSaqueLocal(novoSaque, deveMostrarAviso) {
    const equipe = ladoPorEquipeOuNome(novoSaque);
    if (equipe !== "A" && equipe !== "B") return;

    saqueAtual = equipe;
    ultimoSaqueLocalForcado = equipe;
    saqueProtegidoAte = Date.now() + 9000;
    if (estadoAtual) estadoAtual.saque_atual = equipe;

    if (saqueInfo) saqueInfo.textContent = textoSaqueAtual();
    if (mobileEls && mobileEls.saque) mobileEls.saque.textContent = textoSaqueAtual();

    if (deveMostrarAviso) {
        try { mostrarPopupNovoSaque(equipe, "local"); } catch (e) {}
    }
}

function aplicarPontoOtimista(equipe, scout) {
    equipe = String(equipe || "").toUpperCase();
    if (equipe !== "A" && equipe !== "B") return;

    const agoraClique = Date.now();
    // Trava curta apenas contra duplo toque real. Não bloqueia a regra da rotação.
    if ((agoraClique - ultimoCliquePontoEm) < 280) return;
    ultimoCliquePontoEm = agoraClique;
    sequenciaPontoLocal += 1;

    const resultadoRotacao = aplicarRegraSaqueERotacao(equipe);

    const atualA = numeroInteiro(estadoAtual?.pontos_a ?? estadoAtual?.placar_a ?? 0, 0);
    const atualB = numeroInteiro(estadoAtual?.pontos_b ?? estadoAtual?.placar_b ?? 0, 0);
    const novoA = equipe === "A" ? atualA + 1 : atualA;
    const novoB = equipe === "B" ? atualB + 1 : atualB;

    estadoAtual = {
        ...(estadoAtual || {}),
        pontos_a: novoA,
        pontos_b: novoB,
        placar_a: novoA,
        placar_b: novoB,
        saque_atual: saqueAtual,
        rotacao_a: Array.isArray(rotA) ? [...rotA] : [],
        rotacao_b: Array.isArray(rotB) ? [...rotB] : [],
        ultimo_sideout_girou: !!resultadoRotacao.girou,
        saque_anterior: resultadoRotacao.saqueAnterior || ""
    };

    // Não finaliza set/partida só pelo estado otimista do navegador.
    // A finalização oficial vem do backend após gravar o ponto; isso evita
    // MATCH POINT/SET POINT fora de hora e evita finalizar/rodar duas vezes.
    const vencedorSetLocal = "";

    renderPainelVisual(estadoAtual);
    renderQuadra();
    renderMobileQuadra();

    ultimoTokenSync++;
    placarProtegidoAte = Date.now() + 4500;
    rotacaoProtegidaAte = Date.now() + 9000;
    bloquearSyncAte = Date.now() + 4500;

    const detalhe = scout && scout.detalhe_lance ? tituloBonitoScout(scout.detalhe_lance) : "ponto";
    const msgTempoReal = `Registrando ${detalhe} para ${nomeEquipePorLado(equipe)}.`;
    atualizarUltimaAcao(msgTempoReal);
    estadoAtual.ultima_acao = msgTempoReal;

    emitirEstadoTempoReal({
        ultima_acao: msgTempoReal,
        tipo_evento: "ponto",
        equipe_pontuadora: equipe,
        sideout_girou: !!resultadoRotacao.girou,
        saque_anterior: resultadoRotacao.saqueAnterior || "",
        saque_atual: saqueAtual,
        rotacao_a: Array.isArray(rotA) ? [...rotA] : [],
        rotacao_b: Array.isArray(rotB) ? [...rotB] : []
    });

    salvarSnapshotLocalAutomatico("ponto");
    // autosave de ponto removido: o ponto é salvo pela rota oficial /ponto.
}

const FILA_OFFLINE_KEY = `fila_offline_jogo_${PARTIDA_ID}`;
const URLS_ACAO = {
    ponto: BOOT.urls.ponto,
    tempo: BOOT.urls.tempo,
    substituicao: BOOT.urls.substituicao,
    substituicao_excepcional: BOOT.urls.substituicaoExcepcional,
    retardamento: BOOT.urls.retardamento,
    sancao: BOOT.urls.sancao,
    cartao_verde: BOOT.urls.cartaoVerde,
    sincronizar: BOOT.urls.sincronizar,
    estado: BOOT.urls.estado,
    salvar_estado: BOOT.urls.salvarEstado,
    encerrar: BOOT.urls.encerrar
};


const btnSalvarPartida = document.getElementById("btn-salvar-partida");
const btnSalvarSairPartida = document.getElementById("btn-salvar-sair-partida");

function montarEstadoManualParaBanco() {
    return {
        ...(estadoAtual || {}),
        pontos_a: numeroInteiro(estadoAtual?.pontos_a ?? estadoAtual?.placar_a ?? 0, 0),
        pontos_b: numeroInteiro(estadoAtual?.pontos_b ?? estadoAtual?.placar_b ?? 0, 0),
        placar_a: numeroInteiro(estadoAtual?.pontos_a ?? estadoAtual?.placar_a ?? 0, 0),
        placar_b: numeroInteiro(estadoAtual?.pontos_b ?? estadoAtual?.placar_b ?? 0, 0),
        sets_a: numeroInteiro(estadoAtual?.sets_a ?? 0, 0),
        sets_b: numeroInteiro(estadoAtual?.sets_b ?? 0, 0),
        set_atual: numeroInteiro(estadoAtual?.set_atual ?? 1, 1),
        saque_atual: saqueAtual || estadoAtual?.saque_atual || "",
        rotacao_a: Array.isArray(rotA) ? [...rotA] : [],
        rotacao_b: Array.isArray(rotB) ? [...rotB] : [],
        status_jogadores_a: estadoAtual?.status_jogadores_a || {},
        status_jogadores_b: estadoAtual?.status_jogadores_b || {},
        sancoes_a: estadoAtual?.sancoes_a || [],
        sancoes_b: estadoAtual?.sancoes_b || [],
        cartoes_verdes_a: estadoAtual?.cartoes_verdes_a || [],
        cartoes_verdes_b: estadoAtual?.cartoes_verdes_b || [],
        retardamentos_a: estadoAtual?.retardamentos_a || [],
        retardamentos_b: estadoAtual?.retardamentos_b || [],
        subs_excepcionais: estadoAtual?.subs_excepcionais || [],
        subs_a: numeroInteiro(estadoAtual?.subs_a ?? 0, 0),
        subs_b: numeroInteiro(estadoAtual?.subs_b ?? 0, 0),
        historico: estadoAtual?.historico || [],
        ultima_acao: estadoAtual?.ultima_acao || "Partida salva manualmente"
    };
}

async function salvarPartidaManual(pausar = false) {
    const btn = pausar ? btnSalvarSairPartida : btnSalvarPartida;
    const textoOriginal = btn ? btn.textContent : "";

    limparErro();
    salvarSnapshotLocalAutomatico(pausar ? "salvar_e_sair_clique" : "salvar_manual_clique");

    if (pausar) {
        if (btn) { btn.disabled = true; btn.textContent = "Salvando..."; }
        atualizarUltimaAcao("Partida salva localmente. Enviando ao banco em segundo plano...");

        enviarSnapshotSaidaRapida({
            pausar: true,
            finalizar: false,
            motivo: "salvar_e_sair"
        });

        setTimeout(() => {
            window.location.href = BOOT.urls.painelCompeticao;
        }, 350);
        return;
    }

    try {
        if (btn) { btn.disabled = true; btn.textContent = "Salvando..."; }

        const ok = await enviarSnapshotBancoAutomatico({
            pausar: false,
            finalizar: false,
            motivo: "salvar_manual",
            forcar: true
        });

        if (!ok) throw new Error("Não confirmou no banco agora. O snapshot ficou salvo neste dispositivo e será reenviado na próxima ação.");

        atualizarUltimaAcao("Partida salva no banco.");
        if (btn) btn.textContent = "Salvo";
        setTimeout(() => { if (btn) btn.textContent = textoOriginal; }, 1200);
    } catch (e) {
        mostrarErro(e.message || "Erro ao salvar partida.");
        if (btn) btn.textContent = textoOriginal;
    } finally {
        if (btn) btn.disabled = false;
    }
}

if (btnSalvarPartida) btnSalvarPartida.addEventListener("click", () => salvarPartidaManual(false));
if (btnSalvarSairPartida) btnSalvarSairPartida.addEventListener("click", () => salvarPartidaManual(true));

const OFFLINE_PARTIDAS_KEY = "voleitable_offline_partidas";
const OFFLINE_SESSAO_KEY = "voleitable_offline_sessao";
const OFFLINE_ESTADO_KEY = `voleitable_offline_estado_partida_${PARTIDA_ID}`;
const OFFLINE_HABILITADO_PELO_SUPERADM = BOOT.offlineHabilitado;
const btnPrepararJogoOffline = OFFLINE_HABILITADO_PELO_SUPERADM
    ? document.getElementById("btn-preparar-jogo-offline")
    : null;

const OFFLINE_STORAGE = window.VTPOfflineStorage;
if (!OFFLINE_STORAGE) {
    throw new Error("Módulo offline-storage.js não foi carregado.");
}

function lerJSONLocal(chave, padrao) {
    return OFFLINE_STORAGE.lerJSONLocal(chave, padrao);
}

function salvarJSONLocal(chave, valor) {
    return OFFLINE_STORAGE.salvarJSONLocal(chave, valor);
}


/* =========================================================
   AUTOSAVE SEGURO E RÁPIDO
   =========================================================
   - Não bloqueia clique do apontador.
   - Salva localmente em milissegundos.
   - Envia snapshot para o banco em segundo plano com limite de frequência.
   - "Salvar partida" espera confirmação.
   - "Salvar e sair" manda snapshot por sendBeacon/keepalive e sai rápido.
*/
const AUTOSAVE_LOCAL_KEY = `voleitable_autosave_partida_${PARTIDA_ID}`;
// Enquanto a primeira leitura oficial não terminar, o HTML recém-aberto pode
// conter um snapshot antigo. Não publicar nem persistir esse estado.
let estadoInicialSincronizado = false;
// Bloqueia qualquer autosave/socket enquanto o fluxo oficial troca de set.
let transicaoSetEmAndamento = false;

const AUTOSAVE = window.VTPAutosave?.criarAutosaveController({
    partidaId: PARTIDA_ID,
    competicao: COMPETICAO,
    equipeA: NOME_EQUIPE_A,
    equipeB: NOME_EQUIPE_B,
    chaveAutosave: AUTOSAVE_LOCAL_KEY,
    chaveEstadoOffline: OFFLINE_ESTADO_KEY,
    offlineHabilitado: OFFLINE_HABILITADO_PELO_SUPERADM,
    intervaloMinimoMs: 7000,
    urlHeartbeat: URL_HEARTBEAT_OPERACAO,
    urlSalvarEstado: URLS_ACAO.salvar_estado,
    urlPainelCompeticao: BOOT.urls.painelCompeticao,
    tokenOperador: OPERADOR_SESSAO_TOKEN,
    dispositivoId: DISPOSITIVO_OPERACIONAL_ID,
    http: HTTP,
    lerJSON: lerJSONLocal,
    salvarJSON: salvarJSONLocal,
    carregarFila: () => carregarFilaOffline(),
    montarEstado: () => montarEstadoManualParaBanco(),
    estadoInicialSincronizado: () => estadoInicialSincronizado,
    transicaoSetEmAndamento: () => transicaoSetEmAndamento
});
if (!AUTOSAVE) throw new Error("Módulo autosave-controller.js não foi carregado.");

function salvarSnapshotLocalAutomatico(motivo = "autosave") { return AUTOSAVE.salvarSnapshotLocal(motivo); }
function enviarSnapshotBancoAutomatico(opcoes = {}) { return AUTOSAVE.enviarSnapshotBanco(opcoes); }
function enviarSnapshotSaidaRapida(opcoes = {}) { return AUTOSAVE.enviarSaidaRapida(opcoes); }

AUTOSAVE.iniciarHeartbeat(15000);

const OFFLINE_GAME = window.VTPOfflineGame?.criarOfflineGameController({
    habilitado: OFFLINE_HABILITADO_PELO_SUPERADM,
    partidaId: PARTIDA_ID,
    competicao: COMPETICAO,
    equipeA: NOME_EQUIPE_A,
    equipeB: NOME_EQUIPE_B,
    operadorNome: BOOT.operadorNome,
    chavePartidas: OFFLINE_PARTIDAS_KEY,
    chaveSessao: OFFLINE_SESSAO_KEY,
    chaveEstado: OFFLINE_ESTADO_KEY,
    lerJSON: lerJSONLocal,
    salvarJSON: salvarJSONLocal,
    numeroInteiro,
    obterEstado: () => estadoAtual,
    obterRotacaoA: () => rotA,
    obterRotacaoB: () => rotB,
    obterSaque: () => saqueAtual,
    obterScout: () => scoutLocalPorLado,
    carregarFila: () => carregarFilaOffline(),
    obterBotao: () => btnPrepararJogoOffline,
    partidaFinalizada: () => partidaFinalizada
});
if (!OFFLINE_GAME) throw new Error("Módulo offline-game-controller.js não foi carregado.");

function salvarJogoOfflineLocal(silencioso = true) { return OFFLINE_GAME.salvarLocal(silencioso); }
function removerJogoOfflineSeFinalizado() { return OFFLINE_GAME.removerSeFinalizado(); }

const FILA_EVENTOS = window.VTPFilaEventos?.criarFilaEventosController({
    storage: OFFLINE_STORAGE,
    http: HTTP,
    chaveFila: FILA_OFFLINE_KEY,
    urlSincronizar: URLS_ACAO.sincronizar,
    numeroInteiro,
    obterSetAtual: () => estadoAtual?.set_atual ?? 1,
    aoSalvar: () => {
        // A fila local serve para o autosave rápido, mas o cache completo só é
        // preparado quando o Super ADM liberou oficialmente o modo offline.
        if (OFFLINE_HABILITADO_PELO_SUPERADM) {
            try { salvarJogoOfflineLocal(true); } catch (e) {}
        }
    }
});
if (!FILA_EVENTOS) throw new Error("Módulo fila-eventos-controller.js não foi carregado.");

function carregarFilaOffline() { return FILA_EVENTOS.carregar(); }
function salvarFilaOffline(fila) { return FILA_EVENTOS.salvar(fila); }
function adicionarFilaOffline(tipo, payload) { return FILA_EVENTOS.adicionar(tipo, payload); }
function descricaoLocal(tipo, payload) { return FILA_EVENTOS.descricao(tipo, payload); }


function dadosSubstituicaoLado(lado) {
    return window.VTPSubstituicoes.dadosSubstituicaoLado(estadoAtual, lado);
}

function validarSubstituicaoRegularLocal(lado, sai, entra) {
    return window.VTPSubstituicoes.validarSubstituicaoRegularLocal(estadoAtual, lado, sai, entra);
}

function registrarVinculoSubstituicaoLocal(lado, sai, entra) {
    return window.VTPSubstituicoes.registrarVinculoSubstituicaoLocal(estadoAtual, lado, sai, entra);
}

function acaoOperacionalPermitida(tipo, payload) {
    const lado = payload?.equipe;

    if (partidaFinalizada) {
        mostrarErro("Partida finalizada.");
        return false;
    }

    if (tipo === "tempo" && !podePedirTempo(lado)) {
        mostrarErro(`A equipe ${nomeEquipePorLado(lado)} não possui mais pedidos de tempo neste set.`);
        atualizarTravasOperacionais();
        return false;
    }

    if (tipo === "substituicao" && !podePedirSubstituicao(lado)) {
        mostrarErro("Limite de substituições atingido para esta equipe neste set.");
        atualizarTravasOperacionais();
        return false;
    }
    if (tipo === "substituicao") {
        const v = validarSubstituicaoRegularLocal(lado, payload.numero_sai, payload.numero_entra);
        if (!v.ok) { mostrarErro(v.mensagem); return false; }
    }

    return true;
}

function aplicarAcaoLocal(tipo, payload, opcoes = {}) {
    jogoTeveAcaoLocal = true;
    payload = payload || {};
    const descricao = descricaoLocal(tipo, payload);
    atualizarUltimaAcao(descricao);

    const atual = carregarFilaOffline().slice(-4).reverse().map(a => ({ descricao: descricaoLocal(a.tipo, a.payload || {}) }));
    renderHistoricoBackend([{ descricao }, ...atual].slice(0, 5));

    if (tipo === "tempo") {
        iniciarCronometro(30, payload.equipe);
        const campo = payload.equipe === "A" ? "tempos_a" : "tempos_b";
        estadoAtual[campo] = numeroInteiro(estadoAtual[campo], 0) + 1;
        renderPainelVisual(estadoAtual);
    }

    if (tipo === "substituicao" || tipo === "substituicao_excepcional") {
        const lado = payload.equipe;
        const sai = String(payload.numero_sai || "").trim();
        const entra = String(payload.numero_entra || "").trim();
        if (tipo === "substituicao") registrarVinculoSubstituicaoLocal(lado, sai, entra);
        if (lado === "A" && rotA.length === 6) {
            rotA = rotA.map(n => String(n) === sai ? entra : n);
            if (tipo === "substituicao") estadoAtual.subs_a = numeroInteiro(estadoAtual.subs_a, 0) + 1;
        }
        if (lado === "B" && rotB.length === 6) {
            rotB = rotB.map(n => String(n) === sai ? entra : n);
            if (tipo === "substituicao") estadoAtual.subs_b = numeroInteiro(estadoAtual.subs_b, 0) + 1;
        }
        if (tipo === "substituicao_excepcional") {
            const campo = lado === "A" ? "substituicoes_excepcionais_a" : "substituicoes_excepcionais_b";
            estadoAtual[campo] = [...(Array.isArray(estadoAtual[campo]) ? estadoAtual[campo] : []), payload];
        }
        estadoAtual.rotacao_a = rotA;
        estadoAtual.rotacao_b = rotB;
        renderPainelVisual(estadoAtual);
        renderQuadra();
    }

    if (tipo === "sancao") {
        const categoria = String(payload.tipo_sancao || payload.sancao || "").toLowerCase();
        if (categoria === "advertencia") {
            const campo = payload.equipe === "A" ? "sancoes_a" : "sancoes_b";
            estadoAtual[campo] = [...(Array.isArray(estadoAtual[campo]) ? estadoAtual[campo] : []), payload];
        } else if (categoria === "penalidade") {
            const campo = payload.equipe === "A" ? "cartoes_vermelhos_a" : "cartoes_vermelhos_b";
            estadoAtual[campo] = [...(Array.isArray(estadoAtual[campo]) ? estadoAtual[campo] : []), payload];
            const adversario = payload.equipe === "A" ? "B" : "A";
            aplicarPontoOtimista(adversario, { tipo_lance:"penalidade", detalhe_lance:"penalidade", equipe_responsavel:payload.equipe });
        } else {
            const campo = payload.equipe === "A" ? "outras_sancoes_a" : "outras_sancoes_b";
            estadoAtual[campo] = [...(Array.isArray(estadoAtual[campo]) ? estadoAtual[campo] : []), payload];
        }
        renderPainelVisual(estadoAtual);
    }
    if (tipo === "retardamento") {
        const campo = payload.equipe === "A" ? "retardamentos_a" : "retardamentos_b";
        estadoAtual[campo] = [...(Array.isArray(estadoAtual[campo]) ? estadoAtual[campo] : []), payload];
        renderPainelVisual(estadoAtual);
    }

    if (tipo === "cartao_verde") {
        const campo = payload.equipe === "A" ? "cartoes_verdes_a" : "cartoes_verdes_b";
        const lista = Array.isArray(estadoAtual[campo]) ? estadoAtual[campo].slice() : [];
        lista.push(payload);
        estadoAtual[campo] = lista;
        renderPainelVisual(estadoAtual);
    }


    estadoAtual.ultima_acao = descricao;
    emitirEstadoTempoReal({ ultima_acao: descricao, tipo_evento: tipo, tipo: tipo === "tempo" ? "tempo_autorizado" : tipo, status: tipo === "tempo" ? "iniciado" : "ok", origem: "apontador", equipe: payload.equipe || "", equipe_nome: nomeEquipePorLado(payload.equipe || "") });
    atualizarTravasOperacionais();

    salvarSnapshotLocalAutomatico(tipo || "acao");
    // Ação operacional fica somente no dispositivo até o encerramento.
    // Não agendar autosave no banco aqui.
}

let filaConfirmacaoAcoes = Promise.resolve();
let sequenciaAcaoOtimista = 0;

async function enviarAcaoRapida(tipo, payload, aoSucesso) {
    payload = payload || {};
    limparErro();

    if (!acaoOperacionalPermitida(tipo, payload)) return false;

    // Offline-first real: aplica, transmite por Socket.IO e guarda na fila local.
    // Nenhuma rota HTTP e nenhum acesso ao banco ocorre durante a partida.
    aplicarAcaoLocal(tipo, payload, { semAutosave: true });
    adicionarFilaOffline(tipo, payload);
    placarProtegidoAte = Date.now() + 6000;

    if (typeof aoSucesso === "function") {
        try { aoSucesso({ ok: true, local: true, persistencia: "encerramento" }); } catch (e) {}
    }
    return true;
}

async function sincronizarSetEmSegundoPlano(setNumero, usarKeepalive = false) {
    return FILA_EVENTOS.sincronizarSet(setNumero, usarKeepalive);
}

async function sincronizarFilaOffline() {
    return FILA_EVENTOS.sincronizarTudo();
}

FILA_EVENTOS.registrarReconexao();


function inverterLadosAutomaticamenteParaNovoSet(setEncerrado) {
    const numeroSet = numeroInteiro(setEncerrado, 0);
    if (!numeroSet) return;

    // Cada encerramento de set troca os lados uma única vez. A marca fica no
    // estado local para impedir inversão duplicada em clique/reload/reconexão.
    const ultima = numeroInteiro(estadoAtual?.ultima_inversao_automatica_set, 0);
    if (ultima >= numeroSet) return;

    ladosInvertidos = !ladosInvertidos;
    window.ladosInvertidos = ladosInvertidos;
    estadoAtual.ultima_inversao_automatica_set = numeroSet;
    estadoAtual.lados_invertidos_apontador = !!ladosInvertidos;
    estadoAtual.lados_invertidos = !!ladosInvertidos;
    estadoAtual.quadra_invertida = !!ladosInvertidos;

    localStorage.setItem(CHAVE_INVERSAO_QUADRA, ladosInvertidos ? "1" : "0");
    avisarPlacarAoVivoSobreInversao();
    atualizarEscudosVisuais();
}

async function registrarPonto(equipe, scout) {
    if (enviando || partidaFinalizada) return;

    jogoTeveAcaoLocal = true;
    enviando = true;
    travarBotoes(true);
    limparErro();

    const scoutSeguro = scout || {};

    try {
        // 1) No primeiro ponto, inicia o relógio real local imediatamente.
        garantirInicioTempoRealLocal();

        // 2) Atualiza a tela imediatamente, mas sem decidir fim de set no navegador.
        aplicarScoutLocal(equipe, scoutSeguro);
        aplicarPontoOtimista(equipe, scoutSeguro);
        fecharModalPonto();

        const modoOperacaoIntegralLocal = new URLSearchParams(window.location.search).get("local") === "1";
        if (modoOperacaoIntegralLocal) {
            adicionarFilaOffline("ponto", {
                equipe: equipe,
                fundamento: scoutSeguro.fundamento || scoutSeguro.detalhe_lance || "",
                resultado: scoutSeguro.resultado || scoutSeguro.tipo_lance || "",
                tipo_lance: scoutSeguro.tipo_lance || scoutSeguro.resultado || "",
                detalhe_lance: scoutSeguro.detalhe_lance || scoutSeguro.fundamento || "",
                tipo_erro: scoutSeguro.tipo_erro || "",
                atleta_numero: scoutSeguro.atleta_numero || "",
                atleta_nome: scoutSeguro.atleta_nome || "",
                atleta_label: scoutSeguro.atleta_label || ""
            });
            estadoAtual.ultima_acao = `Ponto para ${nomeEquipePorLado(equipe)}`;
            atualizarUltimaAcao(estadoAtual.ultima_acao);
            emitirEstadoTempoReal({ ...estadoAtual, tipo_evento: "ponto", origem: "apontador_local" });
            salvarSnapshotLocalAutomatico("ponto");

            const pa = numeroInteiro(estadoAtual.pontos_a ?? estadoAtual.placar_a, 0);
            const pb = numeroInteiro(estadoAtual.pontos_b ?? estadoAtual.placar_b, 0);
            const setAtualLocal = numeroInteiro(estadoAtual.set_atual, 1);
            const alvo = (String(estadoAtual.sets_tipo || "").toLowerCase() !== "set_unico" &&
                          setAtualLocal >= (String(estadoAtual.sets_tipo || "").toLowerCase() === "melhor_de_5" ? 5 : 3))
                          ? numeroInteiro(estadoAtual.pontos_tiebreak, 15)
                          : numeroInteiro(estadoAtual.pontos_set, 25);
            const diferenca = Math.abs(pa - pb);
            if (Math.max(pa, pb) >= alvo && diferenca >= numeroInteiro(estadoAtual.diferenca_minima, 2)) {
                const vencedor = pa > pb ? "A" : "B";
                estadoAtual[vencedor === "A" ? "sets_a" : "sets_b"] = numeroInteiro(estadoAtual[vencedor === "A" ? "sets_a" : "sets_b"], 0) + 1;
                adicionarFilaOffline("fim_set", { equipe: vencedor, set_numero: setAtualLocal, pontos_a: pa, pontos_b: pb });
                const venceuJogo = numeroInteiro(estadoAtual[vencedor === "A" ? "sets_a" : "sets_b"], 0) >= numeroInteiro(estadoAtual.sets_para_vencer, 2);
                if (venceuJogo) {
                    estadoAtual.status_jogo = "finalizada";
                    estadoAtual.fase_partida = "encerrado";
                    estadoAtual.fim_jogo = true;
                    estadoAtual.partida_finalizada = true;
                    await abrirObservacoesAutomaticamente(estadoAtual);
                } else {
                    inverterLadosAutomaticamenteParaNovoSet(setAtualLocal);
                    estadoAtual.set_atual = setAtualLocal + 1;
                    estadoAtual.pontos_a = estadoAtual.placar_a = 0;
                    estadoAtual.pontos_b = estadoAtual.placar_b = 0;
                    estadoAtual.tempos_a = estadoAtual.tempos_b = 0;
                    estadoAtual.subs_a = estadoAtual.subs_b = 0;
                    try {
                        const chave=`voleitable_operacao_local_${COMPETICAO}_${PARTIDA_ID}`;
                        const pacote=JSON.parse(localStorage.getItem(chave)||"{}"); pacote.set_atual=estadoAtual.set_atual; pacote.estado=estadoAtual; localStorage.setItem(chave,JSON.stringify(pacote));
                        const eq=pacote.equipes_operacionais||{};
                        window.location.href=`${BOOT.urls.papeleta}?local=1&set=${estadoAtual.set_atual}&equipe_a=${encodeURIComponent(eq.A||estadoAtual.equipe_a||'')}&equipe_b=${encodeURIComponent(eq.B||estadoAtual.equipe_b||'')}`;
                    } catch(e) {}
                }
            }
            return;
        }

        // Compatibilidade do fluxo antigo: fora do modo local integral, usa a rota oficial.
        // Cancela a requisição se o servidor não responder em tempo razoável.
        // Isso impede que a flag `enviando` permaneça presa indefinidamente.
        const controladorPonto = new AbortController();
        const timeoutPonto = window.setTimeout(() => controladorPonto.abort(), 12000);

        let resp;
        try {
            const requisicaoPonto = await window.VTPPontosAPI.registrarPontoOficial(
                HTTP,
                URLS_ACAO.ponto,
                equipe,
                scoutSeguro,
                {
                    saqueAtual,
                    rotacaoA: rotA,
                    rotacaoB: rotB,
                    dispositivoId: DISPOSITIVO_OPERACIONAL_ID
                },
                { signal: controladorPonto.signal }
            );
            resp = requisicaoPonto.resposta;
            resp.__vtpDados = requisicaoPonto.dados;
        } finally {
            window.clearTimeout(timeoutPonto);
        }

        const dados = resp.__vtpDados || {};

        if (!resp.ok || dados.ok === false) {
            throw new Error(dados.mensagem || dados.erro || "Não foi possível registrar o ponto no banco.");
        }

        // 3) Aplica a resposta oficial. Esse payload também alimenta árbitros,
        // treinador e telão pelo socket emitido no backend.
        dados.fonte = "resposta_ponto_fetch";
        aplicarEstado(dados, { fonte: "resposta_ponto_fetch" });

        // Fallback obrigatório: mesmo que algum componente visual lance erro,
        // o fim de set confirmado pelo backend sempre segue para a próxima etapa.
        if (dados.fim_set === true && dados.partida_finalizada !== true && dados.fim_jogo !== true) {
            transicaoSetEmAndamento = true;
            if (autosaveBancoTimer) { clearTimeout(autosaveBancoTimer); autosaveBancoTimer = null; }
            travarBotoes(true);
            const destinoSet = dados.url_redirecionamento
                || dados.url
                || (dados.redirecionar_tiebreak ? BOOT.urls.tiebreak : null)
                || BOOT.urls.papeleta;
            window.setTimeout(() => window.location.replace(destinoSet), 80);
            return;
        }

        const msg = dados.ultima_acao || dados.mensagem || "Ponto registrado";
        atualizarUltimaAcao(msg);
        estadoAtual.ultima_acao = msg;

        // No fluxo oficial, o último ponto já fez COMMIT e marcou a partida
        // como finalizada. Seguimos direto para observações, sem uma segunda
        // chamada /encerrar que poderia reaplicar snapshot local antigo.
        const terminouOficialmente = Boolean(
            dados.fim_jogo === true
            || dados.partida_finalizada === true
            || dados.encerrado === true
            || dados.abrir_observacoes === true
            || ["finalizada", "encerrado"].includes(String(dados.status_jogo || "").trim().toLowerCase())
        );

        if (terminouOficialmente) {
            partidaFinalizada = true;
            travarBotoes(true);
            finalizacaoController.redirecionarObservacoesOficiais(dados, 120);
            return;
        }

    } catch (e) {
        const abortada = e && e.name === "AbortError";

        // Em timeout não adicionamos automaticamente outro ponto na fila,
        // porque o servidor pode ter concluído o COMMIT antes da resposta cair.
        // Isso evita duplicar o ponto ao sincronizar depois.
        if (!abortada) {
            adicionarFilaOffline("ponto", {
                equipe: equipe,
                scout: scoutSeguro,
                estado_depois: montarPayloadTempoReal({ tipo_evento: "ponto" })
            });
            salvarSnapshotLocalAutomatico("ponto_pendente_offline");
        }

        mostrarErro(
            abortada
                ? "O servidor demorou para confirmar. Aguarde alguns segundos e confira o placar antes de tentar novamente."
                : ((e && e.message) ? e.message : "Erro ao registrar ponto.")
        );
        atualizarUltimaAcao(
            abortada
                ? "Confirmação do ponto demorou."
                : "Ponto pendente de sincronização."
        );
    } finally {
        enviando = false;
        if (!partidaFinalizada) {
            travarBotoes(false);
        }
    }
}

    async function registrarSubstituicao() {
        return substituicaoController.confirmar();
    }

    function confirmarPontoAvancado() {
        if (!podeConfirmarPontoAvancado()) {
            mostrarErro("Complete o scout antes de confirmar.");
            return;
        }

        const scout = {
            fundamento: detalheLancePontoTemp || null,
            resultado: tipoLancePontoTemp || null,
            tipo_erro: tipoLancePontoTemp === "erro" ? (detalheLancePontoTemp || null) : null,
            tipo_lance: tipoLancePontoTemp || null,
            detalhe_lance: detalheLancePontoTemp || null,
            equipe_pontuadora: equipePontoTemp,
            equipe_responsavel: tipoLancePontoTemp === "ponto" ? equipePontoTemp : ladoOponente(equipePontoTemp),
            responsavel_lado: tipoLancePontoTemp === "ponto" ? equipePontoTemp : ladoOponente(equipePontoTemp),
            atleta_numero: atletaNumeroPontoTemp || null,
            atleta_nome: atletaNomePontoTemp || null,
            atleta_label: atletaLabelPontoTemp || null
        };

        // A equipe enviada para registrarPonto é SEMPRE quem ganhou o ponto.
        // Erro/falta só muda o responsável do scout, não o placar.
        const equipe = equipePontoTemp;
        registrarPonto(equipe, scout);
    }

    async function desfazerAcao() {
        if (enviando) return;

        enviando = true;
        travarBotoes(true);
        limparErro();

        try {
            const { resposta, dados: dadosBrutos } = await window.VTPPontosAPI.desfazerAcaoOficial(
                HTTP,
                BOOT.urls.desfazer
            );
            const dados = normalizarRespostaJogo(dadosBrutos);

            if (!resposta.ok || !dados.ok) {
                mostrarErro(dados.mensagem || "Erro ao desfazer.");
                return;
            }

            ultimoTokenSync++;
            aplicarEstado(normalizarRespostaJogo({ ...dados, desfazer: true, fonte: "desfazer_fetch", origem: "DESFAZER" }), { fonte: "desfazer_fetch" });
        } catch (e) {
            mostrarErro("Falha ao desfazer ação.");
        } finally {
            enviando = false;
            travarBotoes(false);
        }
    }

    async function registrarTempo(equipe) {
        return window.VTPTempos.registrarTempo(equipe, {
            partidaFinalizada: () => partidaFinalizada,
            limparErro,
            podePedirTempo,
            mostrarErro,
            nomeEquipePorLado,
            atualizarTravasOperacionais,
            enviarAcaoRapida
        });
    }



    function tratarCliquePonto(equipe) {
        limparErro();
        if (enviando || partidaFinalizada) return;

        // MODO SIMPLES: não abre scout/justificativa.
        // Marca o ponto direto, como placar tradicional.
        if (String(modoOperacao || "simples").toLowerCase() !== "avancado") {
            registrarPonto(equipe, {
                tipo_lance: "ponto_simples",
                resultado: "ponto_simples",
                fundamento: "",
                detalhe_lance: "",
                tipo_erro: "",
                atleta_numero: "",
                atleta_nome: "",
                atleta_label: "",
                responsavel_lado: ""
            });
            return;
        }

        // MODO AVANÇADO: abre scout completo.
        abrirModalPonto(equipe);
    }

    function tratarAtalhosModalPonto(event) {
        if (modalPontoFundo.style.display !== "flex") return;
        const tecla = String(event.key || "").toLowerCase();

        if (tecla === "escape") {
            event.preventDefault();
            fecharModalPonto();
            return;
        }

        if ((tecla === "enter" || tecla === "return") && podeConfirmarPontoAvancado()) {
            event.preventDefault();
            confirmarPontoAvancado();
            return;
        }

        if (!tipoLancePontoTemp) {
            const mapa = { "p": "ponto", "e": "erro", "f": "falta" };
            if (mapa[tecla]) {
                event.preventDefault();
                tipoLancePontoTemp = mapa[tecla];
                detalheLancePontoTemp = "";
                atletaNumeroPontoTemp = "";
                atletaNomePontoTemp = "";
                atletaLabelPontoTemp = "";
                renderFluxoPonto();
            }
            return;
        }

        if (tipoLancePontoTemp && !detalheLancePontoTemp) {
            const opcoes = opcoesDetalhePorTipo();
            const idx = ["1","2","3","4","5"].indexOf(tecla);
            if (idx >= 0 && opcoes[idx]) {
                event.preventDefault();
                detalheLancePontoTemp = opcoes[idx].valor;
                atletaNumeroPontoTemp = "";
                atletaNomePontoTemp = "";
                atletaLabelPontoTemp = "";
                renderFluxoPonto();
            }
            return;
        }

        if (pontoExigeAtleta() && !atletaNumeroPontoTemp) {
            const opcoes = opcoesAtletaScoutPonto();
            const idx = ["1","2","3","4","5","6"].indexOf(tecla);
            if (idx >= 0 && opcoes[idx]) {
                event.preventDefault();
                atletaNumeroPontoTemp = String(opcoes[idx].valor || "").trim();
                atletaNomePontoTemp = String(opcoes[idx].nome || "").trim();
                atletaLabelPontoTemp = String(opcoes[idx].labelCompleto || opcoes[idx].title || opcoes[idx].valor || "").trim();
                atualizarResumoPonto();
            }
        }
    }


async function registrarWO(equipe) {
    if (enviando || partidaFinalizada) return;

    const nomePerdedora = nomeEquipePorLado(equipe);
    const ladoVencedor = String(equipe || "").toUpperCase() === "A" ? "B" : "A";
    const nomeVencedora = nomeEquipePorLado(ladoVencedor);
    const confirmar = window.confirm(
        `Confirmar WO da equipe ${nomePerdedora}?\n\n` +
        `${nomeVencedora} será declarada vencedora e a partida será encerrada imediatamente.`
    );
    if (!confirmar) return;

    enviando = true;
    travarBotoes(true);
    limparErro();

    try {
        const { resposta, dados: bruto } = await HTTP.enviarJson(
            BOOT.urls.wo,
            { equipe_wo: equipe },
            { cache: "no-store" }
        );

        const dados = normalizarRespostaJogo(bruto);

        if (!resposta.ok || !dados.ok) {
            mostrarErro(dados.mensagem || "Erro ao registrar WO.");
            return;
        }

        atualizarUltimaAcao(dados.ultima_acao || "Partida encerrada por WO");
        aplicarEstado(dados);
        abrirObservacoesAutomaticamente();
    } catch (e) {
        mostrarErro("Erro de conexão ao registrar WO.");
    } finally {
        enviando = false;
        travarBotoes(false);
    }
}

    const substituicaoExcepcionalController = window.VTPSubstituicaoExcepcional.criarController({
        elementos: {
            modalFundo: modalSubExFundo,
            equipeTexto: subExEquipeTexto,
            saiLista: subExSaiLista,
            entraLista: subExEntraLista,
            motivoInput: subExMotivoInput,
            resumo: subExResumo
        },
        partidaFinalizada: () => partidaFinalizada,
        enviando: () => enviando,
        obterRotacao: (lado) => lado === "A" ? rotA : rotB,
        obterAtletas: atletasEquipePorLado,
        obterStatusMapa: (lado) => lado === "A" ? statusJogadoresA : statusJogadoresB,
        mesclarAtletasComRotacao,
        numeroAtletaOperacional,
        nomeEquipePorLado,
        mostrarErro,
        enviarAcao: enviarAcaoRapida
    });

    function abrirModalSubExcepcional(equipe) {
        substituicaoExcepcionalController.abrir(equipe);
    }

    function fecharModalSubExcepcional() {
        substituicaoExcepcionalController.fechar();
    }

    function registrarSubstituicaoExcepcional() {
        return substituicaoExcepcionalController.confirmar();
    }

    btnA.addEventListener("click", function () { tratarCliquePonto(equipeDoBotao(btnA, "esquerda")); });
    btnB.addEventListener("click", function () { tratarCliquePonto(equipeDoBotao(btnB, "direita")); });
    btnDesfazer.addEventListener("click", function () { desfazerAcao(); });
    if (btnWoA) btnWoA.addEventListener("click", function () { registrarWO(equipeDoBotao(btnWoA, "esquerda")); });
    if (btnWoB) btnWoB.addEventListener("click", function () { registrarWO(equipeDoBotao(btnWoB, "direita")); });
    btnTempoA.addEventListener("click", function () { registrarTempo(equipeDoBotao(btnTempoA, "esquerda")); });
    btnTempoB.addEventListener("click", function () { registrarTempo(equipeDoBotao(btnTempoB, "direita")); });
    btnSubA.addEventListener("click", function () { abrirModalSubstituicao(equipeDoBotao(btnSubA, "esquerda")); });
    btnSubB.addEventListener("click", function () { abrirModalSubstituicao(equipeDoBotao(btnSubB, "direita")); });
    btnSancao.addEventListener("click", function () { abrirModalSancao(); });
    btnVerde.addEventListener("click", function () { abrirModalVerde(); });

btnRetardamento.addEventListener("click", function () { abrirModalRetardamento(); });
btnSubExcepcional.addEventListener("click", function () {
    const equipe = window.prompt("Substituição excepcional de qual equipe? Digite A ou B", "A");
    if (!equipe) return;
    const lado = String(equipe).trim().toUpperCase();
    if (lado !== "A" && lado !== "B") return;
    abrirModalSubExcepcional(lado);
});
btnFullscreen.addEventListener("click", alternarTelaCheia);
if (btnAbrirPlacar) btnAbrirPlacar.addEventListener("click", abrirPlacarAoVivo);
if (btnInverterLados) btnInverterLados.addEventListener("click", inverterLadosVisual);

const mobileHeadEsq = document.getElementById("mobile-head-esq");
const mobileHeadDir = document.getElementById("mobile-head-dir");
const mobilePontoEsq = document.getElementById("mobile-ponto-esq");
const mobilePontoDir = document.getElementById("mobile-ponto-dir");
const mobileMenuEsqBtn = document.getElementById("mobile-menu-esq-btn");
const mobileMenuDirBtn = document.getElementById("mobile-menu-dir-btn");
const mobileDesfazer = document.getElementById("mobile-desfazer");
const mobileFullscreen = document.getElementById("mobile-fullscreen");
const mobileSalvar = document.getElementById("mobile-salvar");
const mobileSalvarSair = document.getElementById("mobile-salvar-sair");

function tratarCliquePontoMobile(posicao, ev) {
    if (ev) {
        ev.preventDefault();
        ev.stopPropagation();
    }
    const agora = Date.now();
    if ((agora - ultimaAcaoPontoMobileEm) < 320) return;
    ultimaAcaoPontoMobileEm = agora;
    tratarCliquePonto(mobileLadoPorPosicao(posicao));
}

if (mobileHeadEsq) mobileHeadEsq.addEventListener("click", (ev) => tratarCliquePontoMobile("esq", ev));
if (mobileHeadDir) mobileHeadDir.addEventListener("click", (ev) => tratarCliquePontoMobile("dir", ev));
if (mobilePontoEsq) mobilePontoEsq.addEventListener("click", (ev) => tratarCliquePontoMobile("esq", ev));
if (mobilePontoDir) mobilePontoDir.addEventListener("click", (ev) => tratarCliquePontoMobile("dir", ev));
if (mobileMenuEsqBtn) mobileMenuEsqBtn.addEventListener("click", () => { if (mobileEls.menuDir) mobileEls.menuDir.classList.remove("aberto"); if (mobileEls.menuEsq) mobileEls.menuEsq.classList.toggle("aberto"); });
if (mobileMenuDirBtn) mobileMenuDirBtn.addEventListener("click", () => { if (mobileEls.menuEsq) mobileEls.menuEsq.classList.remove("aberto"); if (mobileEls.menuDir) mobileEls.menuDir.classList.toggle("aberto"); });
if (mobileDesfazer) mobileDesfazer.addEventListener("click", desfazerAcao);
if (mobileFullscreen) mobileFullscreen.addEventListener("click", alternarTelaCheia);
if (btnMobileInverterLados) btnMobileInverterLados.addEventListener("click", inverterLadosVisual);
if (mobileSalvar) mobileSalvar.addEventListener("click", () => salvarPartidaManual(false));
if (mobileSalvarSair) mobileSalvarSair.addEventListener("click", () => salvarPartidaManual(true));

document.querySelectorAll("[data-mobile-close]").forEach((btn) => btn.addEventListener("click", fecharMenusMobile));
document.querySelectorAll("[data-mobile-action]").forEach((btn) => {
    btn.addEventListener("click", function () {
        const action = this.dataset.mobileAction;
        const side = this.dataset.side || "";
        const equipe = side ? mobileLadoPorPosicao(side) : "";
        fecharMenusMobile();
        if (action === "tempo") return registrarTempo(equipe);
        if (action === "sub") return abrirModalSubstituicao(equipe);
        if (action === "subex") return abrirModalSubExcepcional(equipe);
        if (action === "sancao") return abrirModalSancao();
        if (action === "verde") return abrirModalVerde();
        if (action === "retardamento") return abrirModalRetardamento();
        if (action === "wo") return registrarWO(equipe);
    });
});

renderPainelVisual(estadoAtual);
renderQuadra();
renderMobileQuadra();
// Não finalizar set na abertura da tela. Se o banco estiver em fim de set,
// o redirecionamento deve vir do backend/socket, nunca de cálculo local.
atualizarAvisoPoint(estadoAtual);
if (OFFLINE_HABILITADO_PELO_SUPERADM && btnPrepararJogoOffline) {
    btnPrepararJogoOffline.addEventListener("click", function () {
        salvarJogoOfflineLocal(false);
    });
}
setTimeout(async () => {
    const dadosIniciais = await sincronizarEstadoJogo({ forcarInicial: true }).catch(() => null);
    if (!dadosIniciais) {
        // Não publica o HTML antigo. Tenta novamente após a rede estabilizar.
        setTimeout(() => sincronizarEstadoJogo({ forcarInicial: true }).catch(() => {}), 1800);
    }
}, 350);

btnRetardamentoCancelar.addEventListener("click", fecharModalRetardamento);
btnRetardamentoConfirmar.addEventListener("click", registrarRetardamento);

btnSubExCancelar.addEventListener("click", fecharModalSubExcepcional);
btnSubExConfirmar.addEventListener("click", registrarSubstituicaoExcepcional);

    btnSubCancelar.addEventListener("click", fecharModalSubstituicao);
    btnSubConfirmar.addEventListener("click", registrarSubstituicao);

    btnSancaoCancelar.addEventListener("click", fecharModalSancao);
    btnSancaoConfirmar.addEventListener("click", registrarSancao);

    btnVerdeCancelar.addEventListener("click", fecharModalVerde);
    btnVerdeConfirmar.addEventListener("click", registrarCartaoVerde);

    btnPontoCancelar.addEventListener("click", fecharModalPonto);
    btnPontoConfirmar.addEventListener("click", confirmarPontoAvancado);

modalRetardamentoFundo.addEventListener("click", function (event) {
    if (event.target === modalRetardamentoFundo) fecharModalRetardamento();
});

modalSubExFundo.addEventListener("click", function (event) {
    if (event.target === modalSubExFundo) fecharModalSubExcepcional();
});

    modalSubFundo.addEventListener("click", function (event) {
        if (event.target === modalSubFundo) fecharModalSubstituicao();
    });

    modalSancaoFundo.addEventListener("click", function (event) {
        if (event.target === modalSancaoFundo) fecharModalSancao();
    });

    modalVerdeFundo.addEventListener("click", function (event) {
        if (event.target === modalVerdeFundo) fecharModalVerde();
    });

    modalPontoFundo.addEventListener("click", function (event) {
        if (event.target === modalPontoFundo) fecharModalPonto();
    });

    document.addEventListener("visibilitychange", function () {
        if (!document.hidden) agendarSincronizacao(120, true);
    });

    document.addEventListener("keydown", tratarAtalhosModalPonto);

    atualizarContadoresDisciplinares();
    atualizarAvisoPoint({
        pontos_a: parseInt(pontosA.textContent || "0", 10),
        pontos_b: parseInt(pontosB.textContent || "0", 10),
        sets_a: parseInt(setsA.textContent || "0", 10),
        sets_b: parseInt(setsB.textContent || "0", 10),
        set_atual: parseInt(String(setAtual.textContent || "1"), 10)
    });
    renderQuadra();
    atualizarTravasOperacionais();
    agendarSincronizacao(120, true);
})();
