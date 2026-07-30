"use strict";

(function (global) {
    function criarEstadoInicial() {
        return {
            A: { equipe: { pontos: 0, aces: 0, bloqueios: 0, erros_saque: 0, erros_gerais: 0, faltas: 0, tipos_falta: {}, faltas_lista: [] }, atletas_lista: [] },
            B: { equipe: { pontos: 0, aces: 0, bloqueios: 0, erros_saque: 0, erros_gerais: 0, faltas: 0, tipos_falta: {}, faltas_lista: [] }, atletas_lista: [] }
        };
    }

    function criarController(opcoes) {
        const getAtletas = opcoes.getAtletas;
        const numeroInteiro = opcoes.numeroInteiro;
        const normalizarNumero = opcoes.normalizarNumero;
        const estado = criarEstadoInicial();

        function atletasBase(lado) {
            const atletas = getAtletas(String(lado || "").toUpperCase());
            return Array.isArray(atletas) ? atletas : [];
        }

        function garantirAtleta(lado, numero, nome) {
            lado = String(lado || "").toUpperCase();
            if (!estado[lado]) return null;

            numero = normalizarNumero(numero);
            nome = String(nome || "").trim();

            const lista = estado[lado].atletas_lista;
            let item = lista.find((atleta) => normalizarNumero(atleta.numero) === numero && numero);
            if (!item) {
                const base = atletasBase(lado).find((atleta) => normalizarNumero(atleta.numero) === numero);
                item = {
                    numero: numero || (base && base.numero) || "-",
                    nome: nome || (base && base.nome) || "-",
                    pontos: 0,
                    ataques: 0,
                    aces: 0,
                    bloqueios: 0
                };
                lista.push(item);
            }
            return item;
        }

        function aplicar(equipePontuadora, scout) {
            scout = scout || {};

            const equipePontuadoraNorm = String(equipePontuadora || scout.equipe_pontuadora || "").trim().toUpperCase();
            const ladoResponsavel = String(scout.responsavel_lado || scout.equipe_responsavel || equipePontuadoraNorm || "").trim().toUpperCase();
            if (!estado[equipePontuadoraNorm]) return;

            const tipo = String(scout.tipo_lance || scout.resultado || "").trim().toLowerCase();
            const detalhe = String(scout.detalhe_lance || scout.fundamento || scout.tipo_erro || scout.detalhe || "").trim().toLowerCase();
            const ehErro = tipo === "erro" || scout.resultado === "erro";
            const ehFalta = tipo === "falta" || scout.resultado === "falta" || [
                "rede", "invasao", "invasão", "rotacao", "rotação", "conducao", "condução", "dois_toques", "dois toques"
            ].includes(detalhe);
            const ehPontoDireto = tipo === "ponto" && !ehErro && !ehFalta;
            const ladoScout = (ehErro || ehFalta) ? ladoResponsavel : equipePontuadoraNorm;
            if (!estado[ladoScout]) return;

            const equipeScout = estado[ladoScout].equipe;
            if (ehPontoDireto) {
                equipeScout.pontos = numeroInteiro(equipeScout.pontos, 0) + 1;
                if (detalhe.includes("ace")) equipeScout.aces = numeroInteiro(equipeScout.aces, 0) + 1;
                if (detalhe.includes("bloque")) equipeScout.bloqueios = numeroInteiro(equipeScout.bloqueios, 0) + 1;
            }

            if (ehErro) {
                const campo = detalhe.includes("saque") ? "erros_saque" : "erros_gerais";
                equipeScout[campo] = numeroInteiro(equipeScout[campo], 0) + 1;
            }

            if (ehFalta) {
                equipeScout.faltas = numeroInteiro(equipeScout.faltas, 0) + 1;
                if (!equipeScout.tipos_falta || typeof equipeScout.tipos_falta !== "object") equipeScout.tipos_falta = {};
                const chaveFalta = detalhe || "falta";
                equipeScout.tipos_falta[chaveFalta] = numeroInteiro(equipeScout.tipos_falta[chaveFalta], 0) + 1;
                if (!Array.isArray(equipeScout.faltas_lista)) equipeScout.faltas_lista = [];
                equipeScout.faltas_lista.unshift({
                    tipo: chaveFalta,
                    atleta_numero: scout.atleta_numero || "",
                    atleta_nome: scout.atleta_nome || scout.atleta_label || "",
                    equipe: ladoScout,
                    equipe_responsavel: ladoResponsavel,
                    equipe_pontuadora: equipePontuadoraNorm,
                    momento: Date.now()
                });
                equipeScout.faltas_lista = equipeScout.faltas_lista.slice(0, 8);
            }

            const atleta = garantirAtleta(ladoScout, scout.atleta_numero, scout.atleta_nome || scout.atleta_label);
            if (atleta && ehPontoDireto) {
                atleta.pontos = numeroInteiro(atleta.pontos, 0) + 1;
                if (detalhe.includes("ataque")) atleta.ataques = numeroInteiro(atleta.ataques, 0) + 1;
                if (detalhe.includes("ace")) atleta.aces = numeroInteiro(atleta.aces, 0) + 1;
                if (detalhe.includes("bloque")) atleta.bloqueios = numeroInteiro(atleta.bloqueios, 0) + 1;
            }
        }

        return {
            estado,
            aplicar,
            garantirAtleta
        };
    }

    global.VTPScoutController = Object.freeze({ criarController });
})(window);
