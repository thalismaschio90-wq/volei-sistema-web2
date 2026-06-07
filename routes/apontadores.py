from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify, make_response
import threading
import time
import os
from collections import Counter

try:
    from PIL import Image
except Exception:
    Image = None

from banco import (
    criar_tabelas_oficiais,
    conectar,
    listar_competicoes_apontador,
    excluir_apontador_global,
    buscar_oficial_por_cpf,
    cadastrar_oficial,
    criar_apontador,
    cpf_valido,
    somente_digitos,
    listar_partidas,
    buscar_partida_operacional,
    assumir_partida_operacional,
    abandonar_partida_operacional,
    listar_arbitros_competicao,
    salvar_pre_jogo_partida,
    listar_atletas_aprovados_da_equipe,
    atualizar_numero_atleta,
    equipe_ja_conferida,
    marcar_equipe_conferida,
    salvar_papeleta,
    listar_papeleta,
    inicializar_sets_partida,
    registrar_resultado_set,
    salvar_capitao_partida,
    inicializar_jogo_partida,
    buscar_estado_jogo_partida,
    registrar_ponto_partida,
    registrar_wo_partida,
    desfazer_ultima_acao_partida,
    registrar_tempo_partida,
    buscar_tempos_restantes_partida,
    buscar_competicao_por_nome,
    buscar_configuracao_avancada_competicao,
    registrar_substituicao_partida,
    registrar_substituicao_excepcional_partida,
    registrar_retardamento_partida,
    registrar_sancao_partida,
    registrar_cartao_verde_partida,
    resumir_fluxo_oficial_partida,
    papeleta_set_esta_completa, verificar_fim_de_set, finalizar_set_e_avancar,
    salvar_sorteio_tiebreak_partida,
    partida_encerrada,
    precisa_tiebreak,
    verificar_fim_partida, encerrar_partida,
    garantir_estado_partida,
    listar_eventos_partida,
    aplicar_capitaes_padrao_partida,
    listar_atalhos_apontador as banco_listar_atalhos_apontador,
    salvar_atalhos_apontador as banco_salvar_atalhos_apontador,
    garantir_coluna_jogo_avulso_apontador,
    apontador_pode_criar_jogo_avulso,
    definir_permissao_jogo_avulso_apontador,
    garantir_pins_arbitragem_quadras,
    buscar_vinculo_operacional_por_pin,
    garantir_pin_operacional_apontador,
    normalizar_status_partidas_apontador,
    salvar_estado_manual_partida,
    salvar_resultado_manual_partida,
    validar_operador_partida,
    heartbeat_partida_operacional,
    liberar_trava_partida_operacional,
    salvar_liberos_equipe,
    atualizar_atleta_conferencia_apontador,
    listar_dados_finalizacao_partida,
    salvar_destaque_partida,
    gerar_partidas_avanco_competicao,
)
from routes.utils import exigir_perfil, aplicar_placar_exibicao_lista, aplicar_placar_exibicao_partida
from socket_events import (
    emitir_estado_partida,
    emitir_placar_apontador,
    obter_estado_cache,
    atualizar_estado_cache,
    emitir_tempo_executado,
    emitir_substituicao_executada,
)


try:
    from routes.offline_config import offline_global_habilitado
except Exception:
    def offline_global_habilitado():
        return False

def _atualizar_avanco_apos_finalizacao(competicao):
    """Atualiza/cria partidas do Avanço sem travar o apontador.

    É chamada ao abrir o painel e depois de finalizar/lançar resultado, para que
    quartas, semifinais, finais, 3º lugar, Série Ouro/Prata etc. apareçam
    automaticamente quando suas origens forem resolvidas.
    """
    try:
        return gerar_partidas_avanco_competicao(competicao)
    except Exception as e:
        print("AVISO apontador/atualizar_avanco_apos_finalizacao:", repr(e), flush=True)
        return {}


apontadores_bp = Blueprint("apontadores", __name__)

_CACHE_ARBITROS_COMPETICAO = {}
_CACHE_ATLETAS_EQUIPE = {}

# Cache curto para telas pesadas do apontador.
# Evita bater no Neon a cada troca de aba/volta do tablet/celular.
_CACHE_PAINEL_COMPETICAO_APONTADOR = {}
_CACHE_PAINEL_TTL = int(os.environ.get("APONTADOR_PAINEL_CACHE_TTL", "12") or 12)
_TABELAS_OFICIAIS_GARANTIDAS = False


def _agora_cache():
    try:
        return time.time()
    except Exception:
        return 0


def _cache_get(chave, ttl=None):
    ttl = _CACHE_PAINEL_TTL if ttl is None else ttl
    item = _CACHE_PAINEL_COMPETICAO_APONTADOR.get(chave)
    if not item:
        return None
    criado, valor = item
    if (_agora_cache() - criado) > ttl:
        _CACHE_PAINEL_COMPETICAO_APONTADOR.pop(chave, None)
        return None
    return valor


def _cache_set(chave, valor):
    # Limite simples para não crescer sem controle em torneios longos.
    if len(_CACHE_PAINEL_COMPETICAO_APONTADOR) > 80:
        _CACHE_PAINEL_COMPETICAO_APONTADOR.clear()
    _CACHE_PAINEL_COMPETICAO_APONTADOR[chave] = (_agora_cache(), valor)
    return valor


def _limpar_cache_painel_competicao(competicao=None):
    if not competicao:
        _CACHE_PAINEL_COMPETICAO_APONTADOR.clear()
        return
    prefixo = ("painel_competicao", str(competicao or "").strip())
    for chave in list(_CACHE_PAINEL_COMPETICAO_APONTADOR.keys()):
        if isinstance(chave, tuple) and chave[:2] == prefixo:
            _CACHE_PAINEL_COMPETICAO_APONTADOR.pop(chave, None)


def _garantir_tabelas_oficiais_once():
    # Criar/alterar tabela em toda abertura do painel deixava o login/apontador lento.
    # Fazemos uma vez por processo; em erro, não travamos o usuário.
    global _TABELAS_OFICIAIS_GARANTIDAS
    if _TABELAS_OFICIAIS_GARANTIDAS:
        return
    try:
        criar_tabelas_oficiais()
        _TABELAS_OFICIAIS_GARANTIDAS = True
    except Exception as e:
        print("AVISO garantir tabelas oficiais apontador:", repr(e), flush=True)



def _listar_arbitros_competicao_cache(competicao):
    chave = (competicao or "").strip()
    if chave not in _CACHE_ARBITROS_COMPETICAO:
        _CACHE_ARBITROS_COMPETICAO[chave] = listar_arbitros_competicao(competicao) or []
    return _CACHE_ARBITROS_COMPETICAO[chave]


def _listar_atletas_aprovados_cache(equipe, competicao):
    chave = ((competicao or "").strip(), (equipe or "").strip())
    if chave not in _CACHE_ATLETAS_EQUIPE:
        _CACHE_ATLETAS_EQUIPE[chave] = listar_atletas_aprovados_da_equipe(equipe, competicao) or []
    return _CACHE_ATLETAS_EQUIPE[chave]


def _limpar_cache_atletas(equipe=None, competicao=None):
    if not equipe or not competicao:
        _CACHE_ATLETAS_EQUIPE.clear()
        return
    _CACHE_ATLETAS_EQUIPE.pop(((competicao or "").strip(), (equipe or "").strip()), None)


def _normalizar_fase_operacao(fase):
    fase = (fase or "grupos").strip().lower()
    aliases = {
        "grupo": "grupos",
        "classificatoria": "grupos",
        "classificatória": "grupos",
        "classificatorias": "grupos",
        "classificatórias": "grupos",
        "semifinais": "semifinal",
        "semi": "semifinal",
        "semis": "semifinal",
        "finais": "final",
        "finalissima": "final",
        "finalíssima": "final",
    }
    return aliases.get(fase, fase or "grupos")


def _resolver_modo_operacao_partida(competicao, partida=None):
    """Resolve o modo de operação efetivo da partida.

    Prioridade:
    1. regra avançada específica da fase;
    2. regra avançada específica do grupo, quando for fase de grupos;
    3. padrão geral da competição/partida;
    4. simples.
    """
    partida = partida or {}
    modo_padrao = (partida.get("modo_operacao") or "simples").strip().lower()

    try:
        comp = buscar_competicao_por_nome(competicao) or {}
        modo_padrao = (comp.get("modo_operacao") or modo_padrao or "simples").strip().lower()
    except Exception:
        pass

    modo_final = modo_padrao if modo_padrao in {"simples", "avancado"} else "simples"

    try:
        config = buscar_configuracao_avancada_competicao(competicao) or {}
        fases_config = config.get("fases_config") or {}
        regras_avancadas = fases_config.get("regras_avancadas") or {}
        origem_partida = str(partida.get("origem") or "").strip()
        if origem_partida.startswith("avanco:"):
            partes = origem_partida.split(":")
            serie_id = partes[1] if len(partes) > 1 else ""
            jogo_id = partes[2] if len(partes) > 2 else ""
            regra_jogo = (regras_avancadas.get("jogos") or {}).get(f"{serie_id}:{jogo_id}") or {}
            modo_jogo = (regra_jogo.get("modo_operacao") or "").strip().lower()
            if modo_jogo in {"simples", "avancado"}:
                return modo_jogo
            regra_serie = (regras_avancadas.get("series") or {}).get(serie_id) or {}
            modo_serie = (regra_serie.get("modo_operacao") or "").strip().lower()
            if modo_serie in {"simples", "avancado"}:
                return modo_serie

        fase_id = _normalizar_fase_operacao(partida.get("fase"))

        regra_fase = (regras_avancadas.get("fases") or {}).get(fase_id) or {}
        modo_fase = (regra_fase.get("modo_operacao") or "").strip().lower()
        if modo_fase in {"simples", "avancado"}:
            return modo_fase

        if fase_id == "grupos":
            grupo = (partida.get("grupo") or "").strip().upper()
            regra_grupo = (regras_avancadas.get("grupos") or {}).get(grupo) or {}
            modo_grupo = (regra_grupo.get("modo_operacao") or "").strip().lower()
            if modo_grupo in {"simples", "avancado"}:
                return modo_grupo
    except Exception as e:
        print("AVISO resolver modo operação partida:", repr(e), flush=True)

    return modo_final



def _sets_max_competicao(competicao):
    try:
        comp = buscar_competicao_por_nome(competicao) or {}
        sets_tipo = str(comp.get("sets_tipo") or "melhor_de_3").strip().lower()
    except Exception:
        sets_tipo = "melhor_de_3"

    if sets_tipo == "set_unico":
        return 1
    if sets_tipo == "melhor_de_5":
        return 5
    return 3


def _sets_para_vencer_competicao(competicao):
    sets_max = _sets_max_competicao(competicao)
    if sets_max == 5:
        return 3
    if sets_max == 3:
        return 2
    return 1


def _coletar_sets_form_manual():
    sets = []
    for i in range(1, 6):
        a = request.form.get(f"set{i}_a")
        b = request.form.get(f"set{i}_b")
        if (a is None or str(a).strip() == "") and (b is None or str(b).strip() == ""):
            continue
        sets.append({"a": a, "b": b})
    return sets

# =========================================================
# TECLAS DE ATALHO DO APONTADOR
# =========================================================
ATALHOS_APONTADOR_PADRAO = {
    "ponto_a": "",
    "ponto_b": "",
    "desfazer": "",
    "tempo_a": "",
    "tempo_b": "",
    "substituicao_a": "",
    "substituicao_b": "",
    "sancao": "",
    "cartao_verde": "",
    "retardamento": "",
    "sub_excepcional": "",
    "wo_a": "",
    "wo_b": "",
    "fullscreen": "",
    "placar_ao_vivo": "",
    "inverter_lados": "",
}


def _login_apontador_sessao():
    return (
        session.get("usuario_login")
        or session.get("login")
        or session.get("apontador_login")
        or session.get("usuario")
        or ""
    )



def _validar_operador_http(partida_id, competicao, renovar=True):
    """Proteção central: só o apontador que assumiu a partida pode operar."""
    login = _login_apontador_sessao()
    ok, msg, partida = validar_operador_partida(partida_id, competicao, login, renovar=renovar)
    return ok, msg, partida


def _erro_operador_json(msg, status=423):
    return _json_no_cache({
        "ok": False,
        "bloqueada": True,
        "mensagem": msg or "Esta partida está em operação por outro apontador.",
    }, status)


@apontadores_bp.route("/apontador/atalhos", methods=["GET"])
@exigir_perfil("apontador")
def listar_atalhos_apontador_view():
    login = _login_apontador_sessao()

    try:
        atalhos = dict(ATALHOS_APONTADOR_PADRAO)
        atalhos.update(banco_listar_atalhos_apontador(login) or {})

        return _json_no_cache({
            "ok": True,
            "atalhos": atalhos,
        })

    except Exception as e:
        print("ERRO listar_atalhos_apontador_view:", e)
        return _json_no_cache({
            "ok": False,
            "erro": "Erro ao carregar atalhos do apontador.",
        }, 500)


@apontadores_bp.route("/apontador/atalhos/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_atalhos_apontador_view():
    login = _login_apontador_sessao()
    dados = request.get_json(silent=True) or {}
    atalhos_recebidos = dados.get("atalhos") or {}

    try:
        atalhos = {}
        for acao in ATALHOS_APONTADOR_PADRAO.keys():
            atalhos[acao] = str(atalhos_recebidos.get(acao) or "").strip().upper()

        banco_salvar_atalhos_apontador(login, atalhos)

        return _json_no_cache({
            "ok": True,
            "mensagem": "Atalhos salvos com sucesso.",
        })

    except Exception as e:
        print("ERRO salvar_atalhos_apontador_view:", e)
        return _json_no_cache({
            "ok": False,
            "erro": "Erro ao salvar atalhos do apontador.",
        }, 500)

# =========================================================
# HELPERS
# =========================================================
def _json_no_cache(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"
    return resposta


def _escudo_payload_leve(valor):
    """Evita mandar base64 gigante em polling/estado.

    Escudos base64 continuam salvos e aparecem na renderização inicial, mas o
    endpoint /apontador/estado não precisa reenviar 30KB a cada sincronização.
    """
    valor = str(valor or "").strip()
    if not valor:
        return ESCUDO_PADRAO_URL
    if valor.startswith("data:image") or len(valor) > 500:
        return ESCUDO_PADRAO_URL
    return valor


def _lista_curta(valor, limite=8):
    if not isinstance(valor, list):
        return []
    return valor[:limite]



def _int_seguro(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _limites_operacionais(partida=None, estado=None):
    """
    Centraliza os limites usados pela tela do apontador.
    Prioridade: estado/cache -> partida/regras salvas -> padrão do vôlei.
    """
    partida = partida or {}
    estado = estado or {}

    # A configuração da competição deve ganhar do cache vivo.
    # O cache pode ter nascido com o padrão antigo 2/6 e não pode sobrescrever
    # o que foi programado na competição.
    limite_tempos = _int_seguro(
        partida.get("limite_tempos")
        or partida.get("tempos_limite")
        or partida.get("tempos_por_set")
        or estado.get("limite_tempos")
        or estado.get("tempos_limite")
        or 2,
        2,
    )

    limite_substituicoes = _int_seguro(
        partida.get("limite_substituicoes")
        or partida.get("substituicoes_limite")
        or partida.get("substituicoes_por_set")
        or estado.get("limite_substituicoes")
        or estado.get("substituicoes_limite")
        or 6,
        6,
    )

    return {
        "limite_tempos": max(0, limite_tempos),
        "limite_substituicoes": max(0, limite_substituicoes),
    }


def _contar_eventos_lado(partida_id, competicao, equipe, tipos, set_atual=None):
    """
    Conta ações já salvas para impedir pedidos infinitos.
    Conta por set quando o evento tiver set_numero; se eventos antigos não tiverem,
    ainda assim contabiliza para não liberar infinito.
    """
    equipe = (equipe or "").strip().upper()
    tipos = {str(t or "").strip().lower() for t in (tipos or []) if str(t or "").strip()}
    if equipe not in {"A", "B"} or not tipos:
        return 0

    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=1500) or []
    except TypeError:
        try:
            eventos = listar_eventos_partida(partida_id, competicao) or []
        except Exception:
            return 0
    except Exception:
        return 0

    total = 0
    for ev in eventos:
        ev_equipe = str(ev.get("equipe") or "").strip().upper()
        if ev_equipe != equipe:
            continue

        ev_set = ev.get("set_numero") or ev.get("set") or ev.get("set_atual")
        if set_atual and ev_set not in (None, ""):
            try:
                if int(ev_set) != int(set_atual):
                    continue
            except Exception:
                pass

        campos = {
            str(ev.get("tipo") or "").strip().lower(),
            str(ev.get("tipo_evento") or "").strip().lower(),
            str(ev.get("fundamento") or "").strip().lower(),
            str(ev.get("resultado") or "").strip().lower(),
            str(ev.get("detalhe") or "").strip().lower(),
            str(ev.get("detalhes") or "").strip().lower(),
        }
        campos = {c for c in campos if c}

        if campos.intersection(tipos):
            total += 1

    return total


def _contadores_operacionais(partida_id, competicao, partida=None, estado=None):
    """
    Contadores rápidos do set atual.
    IMPORTANTE: aqui NÃO varremos mais a tabela eventos. Essa varredura era o
    gargalo que fazia tempo/substituição/pedidos travarem por muitos segundos.
    Os contadores vivos ficam no cache/estado e são incrementados de forma
    otimista no clique; o banco salva em seguida.
    """
    estado = estado or {}
    return {
        "tempos_a": _int_seguro(estado.get("tempos_a"), 0),
        "tempos_b": _int_seguro(estado.get("tempos_b"), 0),
        "subs_a": _int_seguro(estado.get("subs_a"), 0),
        "subs_b": _int_seguro(estado.get("subs_b"), 0),
    }

def _aplicar_regras_e_contadores_estado(partida_id, competicao, estado=None, partida=None):
    estado = dict(estado or {})
    partida = dict(partida or {})

    # Regras de tempo/substituição ficam na competição, não na partida.
    # Quando esta função é chamada por ações rápidas, muitas vezes vinha partida={}
    # e a tela caía sempre no padrão 2 tempos / 6 substituições.
    try:
        comp = buscar_competicao_por_nome(competicao) or {}
        for campo in ("tempos_por_set", "substituicoes_por_set", "limite_tempos", "limite_substituicoes", "pontos_set", "pontos_tiebreak", "diferenca_minima", "sets_tipo"):
            if partida.get(campo) in (None, "") and comp.get(campo) not in (None, ""):
                partida[campo] = comp.get(campo)
    except Exception:
        pass

    limites = _limites_operacionais(partida, estado)
    estado["limite_tempos"] = limites["limite_tempos"]
    estado["limite_substituicoes"] = limites["limite_substituicoes"]

    try:
        contadores = _contadores_operacionais(partida_id, competicao, partida=partida, estado=estado)
        # Estes campos representam USADOS no set atual.
        estado["tempos_a"] = contadores["tempos_a"]
        estado["tempos_b"] = contadores["tempos_b"]
        estado["subs_a"] = contadores["subs_a"]
        estado["subs_b"] = contadores["subs_b"]
    except Exception:
        estado.setdefault("tempos_a", 0)
        estado.setdefault("tempos_b", 0)
        estado.setdefault("subs_a", 0)
        estado.setdefault("subs_b", 0)

    # Regras de pontuação para set point/match point no frontend.
    # IMPORTANTE: aqui a regra da competição/partida SEMPRE ganha do cache.
    # O cache do socket_events pode nascer com padrão 25/15/2; se não sobrescrevermos,
    # torneio configurado para 21 nunca mostra SET POINT/MATCH POINT no 20.
    sets_tipo_regra = (partida.get("sets_tipo") or estado.get("sets_tipo") or "melhor_de_3")
    estado["sets_tipo"] = sets_tipo_regra
    estado["pontos_set"] = _int_seguro(
        partida.get("pontos_set")
        or partida.get("ponto_alvo_set")
        or partida.get("pontos_para_vencer_set")
        or estado.get("pontos_set")
        or estado.get("ponto_alvo_set")
        or estado.get("pontos_para_vencer_set")
        or 25,
        25,
    )
    estado["ponto_alvo_set"] = estado["pontos_set"]
    estado["pontos_para_vencer_set"] = estado["pontos_set"]
    estado["pontos_tiebreak"] = _int_seguro(partida.get("pontos_tiebreak") or estado.get("pontos_tiebreak") or 15, 15)
    estado["diferenca_minima"] = _int_seguro(partida.get("diferenca_minima") or estado.get("diferenca_minima") or 2, 2)

    sets_tipo_norm = str(sets_tipo_regra or "").strip().lower()
    if sets_tipo_norm in {"set_unico", "único", "unico", "1_set", "melhor_de_1"}:
        estado["sets_para_vencer"] = 1
    elif sets_tipo_norm == "melhor_de_5":
        estado["sets_para_vencer"] = 3
    elif sets_tipo_norm == "melhor_de_3":
        estado["sets_para_vencer"] = 2
    else:
        estado["sets_para_vencer"] = _int_seguro(partida.get("sets_para_vencer") or estado.get("sets_para_vencer") or 2, 2)

    return estado



# =========================================================
# ESCUDOS / IDENTIDADE VISUAL DAS EQUIPES
# =========================================================
ESCUDO_PADRAO_URL = "/static/img/escudo_padrao.svg"


def _normalizar_url_escudo(valor):
    valor = str(valor or "").strip()
    if not valor:
        return ESCUDO_PADRAO_URL

    if valor.startswith(("http://", "https://", "/static/", "data:")):
        return valor

    valor = valor.replace("\\", "/")

    if valor.startswith("static/"):
        return "/" + valor

    if valor.startswith("uploads/"):
        return "/static/" + valor

    if "/uploads/" in valor:
        parte = valor.split("/uploads/", 1)[1]
        return "/static/uploads/" + parte

    return "/static/uploads/escudos/" + valor.lstrip("/")


def _eh_escudo_padrao(valor):
    normalizado = _normalizar_url_escudo(valor)
    return (
        not valor
        or normalizado == ESCUDO_PADRAO_URL
        or normalizado.endswith('/img/escudo_padrao.svg')
    )


def _buscar_colunas_escudo_equipe():
    """Retorna todas as colunas possíveis de escudo existentes na tabela equipes.

    Importante: algumas versões salvaram em escudo, outras em escudo_url.
    Se buscarmos só a primeira coluna existente, podemos cair numa coluna vazia
    e nunca chegar no arquivo real enviado pela equipe.
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'equipes'
                      AND column_name IN ('escudo_url', 'escudo', 'logo_url', 'logo')
                """)
                existentes = {str(row.get('column_name') or '').strip() for row in (cur.fetchall() or [])}
    except Exception as e:
        print('AVISO colunas escudo equipes:', repr(e), flush=True)
        existentes = set()

    ordem = ['escudo_url', 'escudo', 'logo_url', 'logo']
    return [c for c in ordem if c in existentes]


def _buscar_escudos_equipes(competicao, equipe_a, equipe_b):
    colunas = _buscar_colunas_escudo_equipe()
    nomes = [str(equipe_a or '').strip(), str(equipe_b or '').strip()]
    nomes_validos = [n for n in nomes if n]

    resultado = {n: ESCUDO_PADRAO_URL for n in nomes_validos}

    if not colunas or not nomes_validos:
        return resultado

    # Monta COALESCE(NULLIF(TRIM(escudo_url), ''), NULLIF(TRIM(escudo), ''), ...)
    expr_escudo = 'COALESCE(' + ', '.join([f"NULLIF(TRIM({c}), '')" for c in colunas]) + ") AS escudo"

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT nome, competicao, {expr_escudo}
                    FROM equipes
                    WHERE LOWER(TRIM(nome)) = ANY(%s)
                    ORDER BY
                        CASE WHEN competicao = %s THEN 0 ELSE 1 END,
                        CASE WHEN { 'COALESCE(' + ', '.join([f"NULLIF(TRIM({c}), '')" for c in colunas]) + ')' } IS NULL THEN 1 ELSE 0 END
                """, ([n.lower() for n in nomes_validos], competicao))

                for row in cur.fetchall() or []:
                    nome_banco = str(row.get('nome') or '').strip()
                    escudo = row.get('escudo')
                    if not escudo:
                        continue

                    for original in nomes_validos:
                        if original.lower().strip() == nome_banco.lower().strip():
                            # A primeira linha útil é a da competição atual; depois fallback global.
                            if _eh_escudo_padrao(resultado.get(original)):
                                resultado[original] = _normalizar_url_escudo(escudo)
    except Exception as e:
        print('AVISO buscar escudos equipes:', repr(e), flush=True)

    return resultado



_COR_DOMINANTE_CACHE = {}


def _url_escudo_para_caminho_local(url):
    """Converte /static/... para caminho local no projeto.

    URLs externas/data-uri não entram no cálculo de cor.
    """
    url = _normalizar_url_escudo(url)
    if not url or url.startswith(("http://", "https://", "data:")):
        return None

    if url.startswith("/static/"):
        relativo = url.lstrip("/")
    elif url.startswith("static/"):
        relativo = url
    else:
        return None

    candidatos = [
        os.path.join(os.getcwd(), relativo),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), relativo) if "__file__" in globals() else "",
    ]

    for caminho in candidatos:
        if caminho and os.path.exists(caminho):
            return caminho

    return candidatos[0] if candidatos else None


def _cor_hex_valida(valor):
    valor = str(valor or "").strip()
    if len(valor) != 7 or not valor.startswith("#"):
        return False
    try:
        int(valor[1:], 16)
        return True
    except Exception:
        return False


def _escurecer_cor(hex_cor, fator=0.82):
    if not _cor_hex_valida(hex_cor):
        return hex_cor
    r = int(hex_cor[1:3], 16)
    g = int(hex_cor[3:5], 16)
    b = int(hex_cor[5:7], 16)
    r = max(0, min(255, int(r * fator)))
    g = max(0, min(255, int(g * fator)))
    b = max(0, min(255, int(b * fator)))
    return f"#{r:02x}{g:02x}{b:02x}"


def _cor_dominante_escudo_url(url, fallback="#2E6BE6"):
    """Extrai a cor dominante real do escudo.

    Mantém preto/cinza como cores válidas. Ignora só transparência e branco
    quase puro, porque fundo branco de PNG costuma atrapalhar a leitura.
    """
    url = _normalizar_url_escudo(url)
    if _eh_escudo_padrao(url):
        return fallback

    chave = url
    if chave in _COR_DOMINANTE_CACHE:
        return _COR_DOMINANTE_CACHE[chave]

    caminho = _url_escudo_para_caminho_local(url)
    if Image is None or not caminho or not os.path.exists(caminho):
        _COR_DOMINANTE_CACHE[chave] = fallback
        return fallback

    try:
        with Image.open(caminho) as img:
            img = img.convert("RGBA")
            img.thumbnail((96, 96))

            contagem = Counter()
            for r, g, b, a in img.getdata():
                if a < 135:
                    continue
                # ignora branco/quase branco, mas NÃO ignora preto.
                if r > 235 and g > 235 and b > 235:
                    continue

                # Agrupa tons próximos para evitar antialias pegar uma variação isolada.
                rq = int(round(r / 24.0) * 24)
                gq = int(round(g / 24.0) * 24)
                bq = int(round(b / 24.0) * 24)
                rq = max(0, min(255, rq))
                gq = max(0, min(255, gq))
                bq = max(0, min(255, bq))
                contagem[(rq, gq, bq)] += 1

            if not contagem:
                cor = fallback
            else:
                r, g, b = contagem.most_common(1)[0][0]

                # Preto muito absoluto fica pesado; usa um preto de transmissão.
                if r < 35 and g < 35 and b < 35:
                    cor = "#111827"
                else:
                    cor = f"#{r:02x}{g:02x}{b:02x}"
                    # Evita card claro demais quando o escudo tem amarelo/cinza claro.
                    brilho = (r * 299 + g * 587 + b * 114) / 1000
                    if brilho > 185:
                        cor = _escurecer_cor(cor, 0.62)

        _COR_DOMINANTE_CACHE[chave] = cor
        return cor
    except Exception as e:
        print("AVISO cor dominante escudo:", repr(e), flush=True)
        _COR_DOMINANTE_CACHE[chave] = fallback
        return fallback


def _cor_estado_ou_auto(estado, chave_cor, chave_cor_alt, escudo, fallback):
    valor = str(estado.get(chave_cor) or estado.get(chave_cor_alt) or "").strip()
    # Se veio o fallback antigo, deixa a cor automática do escudo assumir.
    if valor and valor.upper() not in {"#2E6BE6", "#E53935"} and _cor_hex_valida(valor):
        return valor
    return _cor_dominante_escudo_url(escudo, fallback)


def _aplicar_escudos_estado(estado, competicao, equipe_a, equipe_b):
    estado = dict(estado or {})
    equipe_a = str(equipe_a or '').strip()
    equipe_b = str(equipe_b or '').strip()

    escudos_banco = _buscar_escudos_equipes(competicao, equipe_a, equipe_b)

    escudo_a_estado = (
        estado.get('escudo_a_operacional')
        or estado.get('escudo_a')
        or estado.get('equipe_a_escudo')
    )
    escudo_b_estado = (
        estado.get('escudo_b_operacional')
        or estado.get('escudo_b')
        or estado.get('equipe_b_escudo')
    )

    escudo_a_banco = escudos_banco.get(equipe_a)
    escudo_b_banco = escudos_banco.get(equipe_b)

    # Se o estado/cache já veio com escudo padrão, ele NÃO pode ganhar do banco.
    # Isso era o motivo de aparecer o mesmo escudo padrão para os dois times.
    escudo_a = escudo_a_estado if not _eh_escudo_padrao(escudo_a_estado) else escudo_a_banco
    escudo_b = escudo_b_estado if not _eh_escudo_padrao(escudo_b_estado) else escudo_b_banco

    estado['escudo_a'] = _normalizar_url_escudo(escudo_a)
    estado['escudo_b'] = _normalizar_url_escudo(escudo_b)
    estado['escudo_a_operacional'] = estado['escudo_a']
    estado['escudo_b_operacional'] = estado['escudo_b']
    estado['equipe_a_escudo'] = estado['escudo_a']
    estado['equipe_b_escudo'] = estado['escudo_b']

    estado['cor_a'] = _cor_estado_ou_auto(estado, 'cor_a', 'equipe_a_cor', estado['escudo_a'], '#2E6BE6')
    estado['cor_b'] = _cor_estado_ou_auto(estado, 'cor_b', 'equipe_b_cor', estado['escudo_b'], '#E53935')
    estado['cor_a_operacional'] = estado['cor_a']
    estado['cor_b_operacional'] = estado['cor_b']
    estado['equipe_a_cor'] = estado['cor_a']
    estado['equipe_b_cor'] = estado['cor_b']
    return estado


def _validar_limite_operacional(partida_id, competicao, equipe, tipo, partida=None, estado=None):
    equipe = (equipe or "").strip().upper()
    partida = partida or {}
    estado = estado or {}
    estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)

    if tipo == "tempo":
        usados = _int_seguro(estado.get("tempos_a") if equipe == "A" else estado.get("tempos_b"), 0)
        limite = _int_seguro(estado.get("limite_tempos"), 2)
        if usados >= limite:
            return False, f"Limite de tempos atingido para a Equipe {equipe} neste set.", estado

    if tipo == "substituicao":
        usados = _int_seguro(estado.get("subs_a") if equipe == "A" else estado.get("subs_b"), 0)
        limite = _int_seguro(estado.get("limite_substituicoes"), 6)
        if usados >= limite:
            return False, f"Limite de substituições atingido para a Equipe {equipe} neste set.", estado

    return True, "", estado


def _montar_descricao_evento(ev):
    descricao = (ev.get("descricao") or "").strip()
    if descricao:
        return descricao

    partes = []
    tipo_evento = str(ev.get("tipo_evento") or ev.get("tipo") or "").strip()
    equipe = str(ev.get("equipe") or "").strip()
    fundamento = str(ev.get("fundamento") or "").strip()
    resultado = str(ev.get("resultado") or "").strip()
    detalhe = str(ev.get("detalhe") or ev.get("detalhes") or "").strip()
    numero = str(ev.get("numero") or "").strip()
    atleta_nome = str(ev.get("atleta_nome") or "").strip()

    if tipo_evento:
        partes.append(tipo_evento.replace("_", " ").title())
    if equipe:
        partes.append(f"Equipe {equipe}")
    if fundamento:
        partes.append(fundamento.replace("_", " "))
    if resultado:
        partes.append(resultado.replace("_", " "))
    if detalhe:
        partes.append(detalhe.replace("_", " "))
    if numero:
        partes.append(f"#{numero}")
    if atleta_nome:
        partes.append(atleta_nome)

    return " • ".join([p for p in partes if p]) or "Ação registrada"


def _buscar_historico_resumido(partida_id, competicao, limite=5):
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=limite) or []
    except TypeError:
        eventos = listar_eventos_partida(partida_id, competicao) or []
        eventos = eventos[:limite]
    except Exception:
        return [], "-"

    historico = [{"descricao": _montar_descricao_evento(ev)} for ev in eventos]
    ultima_acao = historico[0]["descricao"] if historico else "-"
    return historico[:limite], ultima_acao


def _numero_atleta_operacional(valor):
    """Normaliza número/camisa de atleta para string.

    Várias telas dependem de `numero`, mas alguns registros podem chegar como
    dict da papeleta, int, texto, `numero_camisa` ou `camisa`. Mantém leve e
    sem consulta extra.
    """
    if isinstance(valor, dict):
        valor = (
            valor.get("numero")
            or valor.get("camisa")
            or valor.get("numero_camisa")
            or valor.get("atleta_numero")
            or valor.get("n")
            or ""
        )
    return str(valor or "").strip()


def _normalizar_atleta_operacional(atleta, numero_fallback=""):
    atleta = dict(atleta or {})
    numero = _numero_atleta_operacional(
        atleta.get("numero")
        or atleta.get("camisa")
        or atleta.get("numero_camisa")
        or atleta.get("atleta_numero")
        or numero_fallback
    )
    if not numero:
        return None

    atleta["numero"] = numero
    atleta.setdefault("camisa", numero)
    atleta.setdefault("numero_camisa", numero)
    atleta["nome"] = str(atleta.get("nome") or atleta.get("atleta_nome") or "Atleta").strip() or "Atleta"
    return atleta


def _merge_atletas_operacionais(atletas, papeleta=None, rotacao=None):
    """Garante lista de atletas para modais sem depender só do SELECT de atletas.

    Se o elenco vier vazio/desatualizado, os números da papeleta/rotação ainda
    aparecem na substituição, sanção, cartão verde e scout. Não faz consulta
    pesada; usa somente dados já carregados na rota.
    """
    saida = []
    vistos = set()

    def add(item, numero_fallback=""):
        atleta = _normalizar_atleta_operacional(item, numero_fallback)
        if not atleta:
            return
        numero = atleta["numero"]
        if numero in vistos:
            return
        vistos.add(numero)
        saida.append(atleta)

    for atleta in atletas or []:
        add(atleta)

    if isinstance(papeleta, dict):
        for pos in range(1, 7):
            add({"numero": papeleta.get(pos), "nome": "Atleta"}, papeleta.get(pos))

    if isinstance(rotacao, dict):
        rotacao = rotacao.get("equipe_a") or rotacao.get("equipe_b") or []

    if isinstance(rotacao, (list, tuple)):
        for numero in rotacao:
            add({"numero": numero, "nome": "Atleta"}, numero)

    return saida


def _buscar_papeletas_set_atual(partida_id, competicao, partida, estado=None):
    equipe_a = (
        partida.get("equipe_a_operacional")
        or partida.get("equipe_a")
        or (estado or {}).get("equipe_a_operacional")
        or (estado or {}).get("equipe_a")
    )
    equipe_b = (
        partida.get("equipe_b_operacional")
        or partida.get("equipe_b")
        or (estado or {}).get("equipe_b_operacional")
        or (estado or {}).get("equipe_b")
    )
    set_atual = int(partida.get("set_atual") or (estado or {}).get("set_atual") or 1)

    def carregar_papeleta(equipe_principal, equipe_fallback):
        papeleta = {}
        nomes = []
        for nome in (equipe_principal, equipe_fallback):
            nome = str(nome or "").strip()
            if nome and nome not in nomes:
                nomes.append(nome)

        for nome in nomes:
            try:
                dados = listar_papeleta(partida_id, competicao, nome, set_atual) or []
            except Exception:
                dados = []
            if dados:
                for row in dados:
                    try:
                        pos = int(row.get("posicao") or 0)
                    except Exception:
                        continue
                    if 1 <= pos <= 6:
                        papeleta[pos] = _numero_atleta_operacional(row.get("numero"))
                break

        for i in range(1, 7):
            papeleta.setdefault(i, "")

        return papeleta

    papeleta_a = carregar_papeleta(equipe_a, partida.get("equipe_a"))
    papeleta_b = carregar_papeleta(equipe_b, partida.get("equipe_b"))

    return equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b


def _rotacao_fallback_por_papeleta(papeleta):
    return [
        _numero_atleta_operacional(papeleta.get(4, "")),
        _numero_atleta_operacional(papeleta.get(3, "")),
        _numero_atleta_operacional(papeleta.get(2, "")),
        _numero_atleta_operacional(papeleta.get(5, "")),
        _numero_atleta_operacional(papeleta.get(6, "")),
        _numero_atleta_operacional(papeleta.get(1, "")),
    ]




def _rotacao_tem_atletas_front(rotacao):
    if isinstance(rotacao, dict):
        rotacao = rotacao.get("equipe_a") or rotacao.get("equipe_b") or []
    if not isinstance(rotacao, list):
        return False
    return any(str(x.get("numero") if isinstance(x, dict) else x or "").strip() for x in rotacao)
def _rotacao_segura_estado(estado, lado):
    """
    Garante que a rotação usada no clique do ponto nunca venha vazia.
    Isso evita o bug em que o placar atualiza, mas a rotação não gira porque
    o cache/estado não trouxe rotacao_a ou rotacao_b no formato esperado.
    """
    estado = estado or {}
    lado = (lado or "").strip().upper()

    chave = "rotacao_a" if lado == "A" else "rotacao_b"
    rotacao = estado.get(chave)

    # Compatibilidade com payloads antigos que guardam as rotações dentro de "rotacao".
    if not rotacao and isinstance(estado.get("rotacao"), dict):
        rotacao = estado["rotacao"].get("equipe_a" if lado == "A" else "equipe_b")

    if not isinstance(rotacao, list):
        rotacao = []

    normalizada = []
    for item in rotacao:
        if isinstance(item, dict):
            numero = item.get("numero") or item.get("camisa") or item.get("n") or ""
        else:
            numero = item
        normalizada.append(str(numero or "").strip())

    while len(normalizada) < 6:
        normalizada.append("")

    return normalizada[:6]


def _montar_evolucao_pontos(partida_id, competicao, set_atual=None):
    """
    Monta a evolução ponto a ponto a partir do banco.

    Regra importante:
    - o telão não deve depender da página estar aberta;
    - ao atualizar/sair/voltar, a evolução é reconstruída pelos eventos salvos;
    - quando existir detalhes.equipe_pontuadora, ela é a fonte mais confiável.
    """
    import json

    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=300) or []
    except TypeError:
        try:
            eventos = listar_eventos_partida(partida_id, competicao) or []
        except Exception:
            return []
    except Exception:
        return []

    try:
        set_filtro = int(set_atual) if set_atual not in (None, "") else None
    except Exception:
        set_filtro = None

    def chave_ordem(ev):
        return (
            ev.get("id") or 0,
            str(ev.get("criado_em") or "")
        )

    def detalhes_dict(ev):
        detalhes = ev.get("detalhes") or {}
        if isinstance(detalhes, dict):
            return detalhes
        if isinstance(detalhes, str) and detalhes.strip():
            try:
                obj = json.loads(detalhes)
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}
        return {}

    def lado_pontuador(ev):
        tipo_evento = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        if tipo_evento not in {"ponto", "pontuacao", "pontuação"}:
            return ""

        det = detalhes_dict(ev)

        # Novo padrão salvo por registrar_ponto_partida: sempre aponta quem ganhou o ponto.
        equipe_pontuadora = str(det.get("equipe_pontuadora") or "").strip().upper()
        if equipe_pontuadora in {"A", "B"}:
            return equipe_pontuadora

        equipe = str(ev.get("equipe") or "").strip().upper()
        if equipe not in {"A", "B"}:
            return ""

        fundamento = str(ev.get("fundamento") or "").strip().lower()
        resultado = str(ev.get("resultado") or "").strip().lower()
        tipo_lance = str(
            ev.get("tipo_lance")
            or ev.get("detalhe")
            or det.get("tipo_lance")
            or det.get("resultado")
            or ev.get("detalhes")
            or ""
        ).strip().lower()

        eh_erro_ou_falta = (
            resultado in {"erro", "falta"}
            or tipo_lance in {"erro", "falta"}
            or fundamento in {
                "erro_saque",
                "erro_geral",
                "rede",
                "invasao",
                "invasão",
                "rotacao",
                "rotação",
                "conducao",
                "condução",
                "dois_toques",
                "dois toques",
            }
        )

        eh_ponto_proprio = (
            resultado == "ponto"
            or tipo_lance == "ponto"
            or fundamento in {"ataque", "bloqueio", "ace"}
        )

        if eh_erro_ou_falta:
            return "B" if equipe == "A" else "A"
        if eh_ponto_proprio:
            return equipe

        return ""

    evolucao = []

    for ev in sorted(eventos, key=chave_ordem):
        if set_filtro is not None:
            ev_set = ev.get("set_numero") or ev.get("set_atual") or ev.get("set")
            if ev_set not in (None, ""):
                try:
                    if int(ev_set) != set_filtro:
                        continue
                except Exception:
                    pass

        lado = lado_pontuador(ev)
        if lado in {"A", "B"}:
            evolucao.append(lado)

    return evolucao[-50:]

def _calcular_placar_atual_por_eventos(partida_id, competicao, set_atual=None):
    """
    Calcula o placar real do set atual usando os eventos de ponto.

    Esse é o placar mais confiável quando o cache/coluna da partida fica atrasado
    depois de ações extras como tempo, sanção, substituição ou cartão verde.
    A evolução ponto a ponto já vinha certa porque nasce dos eventos; aqui usamos
    a mesma fonte para não deixar o placar principal voltar para 0/1 indevidamente.
    """
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=200) or []
    except TypeError:
        try:
            eventos = listar_eventos_partida(partida_id, competicao) or []
        except Exception:
            return None
    except Exception:
        return None

    def chave_ordem(ev):
        return (ev.get("id") or 0, str(ev.get("criado_em") or ""))

    pontos_a = 0
    pontos_b = 0
    set_atual_int = None
    try:
        set_atual_int = int(set_atual) if set_atual is not None else None
    except Exception:
        set_atual_int = None

    for ev in sorted(eventos, key=chave_ordem):
        if set_atual_int is not None:
            ev_set = ev.get("set_numero") or ev.get("set_atual") or ev.get("set")
            if ev_set not in (None, ""):
                try:
                    if int(ev_set) != set_atual_int:
                        continue
                except Exception:
                    pass

        tipo_evento = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        equipe = str(ev.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            continue
        if tipo_evento not in {"ponto", "pontuacao", "pontuação"}:
            continue

        fundamento = str(ev.get("fundamento") or "").strip().lower()
        resultado = str(ev.get("resultado") or "").strip().lower()
        tipo_lance = str(
            ev.get("tipo_lance")
            or ev.get("detalhe")
            or ev.get("detalhes")
            or ""
        ).strip().lower()

        eh_ponto_proprio = (
            resultado == "ponto"
            or tipo_lance == "ponto"
            or fundamento in {"ataque", "bloqueio", "ace"}
        )

        eh_erro_ou_falta = (
            resultado in {"erro", "falta"}
            or tipo_lance in {"erro", "falta"}
            or fundamento in {
                "erro_saque",
                "erro_geral",
                "rede",
                "invasao",
                "rotacao",
                "conducao",
                "dois_toques",
            }
        )

        if eh_erro_ou_falta:
            equipe_ponto = "B" if equipe == "A" else "A"
        elif eh_ponto_proprio:
            equipe_ponto = equipe
        else:
            continue

        if equipe_ponto == "A":
            pontos_a += 1
        else:
            pontos_b += 1

    return {"pontos_a": pontos_a, "pontos_b": pontos_b, "total": pontos_a + pontos_b}


def _reconciliar_placar_com_eventos(partida_id, competicao, estado):
    """Nunca deixa resposta/cache atrasado sobrescrever placar real já salvo nos eventos."""
    estado = dict(estado or {})
    set_atual = estado.get("set_atual") or 1
    calculado = _calcular_placar_atual_por_eventos(partida_id, competicao, set_atual)
    if not calculado:
        return estado

    atual_a = int(estado.get("pontos_a") or estado.get("placar_a") or 0)
    atual_b = int(estado.get("pontos_b") or estado.get("placar_b") or 0)
    total_atual = atual_a + atual_b
    total_calc = int(calculado.get("total") or 0)

    # Se os eventos têm mais pontos que o estado atual, o estado está atrasado/zerado.
    # Em ações como tempo/sanção/substituição/cartão, isso era o que fazia o placar cair.
    if total_calc > total_atual:
        estado["pontos_a"] = int(calculado.get("pontos_a") or 0)
        estado["pontos_b"] = int(calculado.get("pontos_b") or 0)
        estado["placar_a"] = estado["pontos_a"]
        estado["placar_b"] = estado["pontos_b"]

    return estado


def _deve_rebuild_pesado_estado(origem="", estado=None, forcar=False):
    """
    Decide quando vale consultar eventos no banco para reconstruir placar/evolução.

    Antes, todo ponto/heartbeat/sync fazia rebuild a partir de eventos. Isso deixa o
    apontador lento e causa atraso visual no saque/rotação. Agora o cache/socket é
    dominante durante o jogo; o rebuild fica para abertura sem cache, desfazer, WO,
    finalização ou quando for explicitamente forçado.
    """
    if forcar:
        return True
    estado = estado or {}
    origem = str(origem or "").upper()
    if estado.get("_forcar_rebuild_eventos") or estado.get("rebuild_eventos"):
        return True
    if origem in {"DESFAZER", "WO", "FINALIZAR", "ENCERRAR", "ABERTURA_SEM_CACHE"}:
        return True
    if origem.startswith("SINCRONIZAR_FINAL"):
        return True
    return False


def _preparar_estado_para_placar(partida_id, competicao, estado=None, partida=None):
    """
    Garante que o payload enviado ao telão sempre tenha:
    - nomes das equipes
    - competição
    - partida_id
    - placar atual
    - evolução ponto a ponto em ordem real
    """
    estado = dict(estado or {})

    if partida is None:
        try:
            partida = buscar_partida_operacional(partida_id, competicao) or {}
        except Exception:
            partida = {}

    estado["competicao"] = estado.get("competicao") or competicao
    estado["partida_id"] = estado.get("partida_id") or partida_id

    estado["equipe_a"] = (
        estado.get("equipe_a")
        or estado.get("equipeA")
        or estado.get("equipe_a_nome")
        or estado.get("nome_equipe_a")
        or estado.get("nome_a")
        or estado.get("time_a")
        or partida.get("equipe_a")
        or partida.get("equipe_a_operacional")
        or ""
    )

    estado["equipe_b"] = (
        estado.get("equipe_b")
        or estado.get("equipeB")
        or estado.get("equipe_b_nome")
        or estado.get("nome_equipe_b")
        or estado.get("nome_b")
        or estado.get("time_b")
        or partida.get("equipe_b")
        or partida.get("equipe_b_operacional")
        or ""
    )

    if "pontos_a" not in estado:
        estado["pontos_a"] = estado.get("placar_a", 0)

    if "pontos_b" not in estado:
        estado["pontos_b"] = estado.get("placar_b", 0)

    if "placar_a" not in estado:
        estado["placar_a"] = estado.get("pontos_a", 0)

    if "placar_b" not in estado:
        estado["placar_b"] = estado.get("pontos_b", 0)

    if _deve_rebuild_pesado_estado(estado=estado):
        estado = _reconciliar_placar_com_eventos(partida_id, competicao, estado)
        estado["evolucao_pontos"] = _montar_evolucao_pontos(
            partida_id,
            competicao,
            estado.get("set_atual") or 1,
        )
    else:
        estado.setdefault("evolucao_pontos", estado.get("evolucao") or [])

    estado = _aplicar_escudos_estado(estado, competicao, estado.get("equipe_a"), estado.get("equipe_b"))

    return estado


def _emitir_estado_e_placar(partida_id, competicao, estado=None, partida=None, origem=""):
    estado = dict(estado or {})

    if partida is None:
        try:
            partida = buscar_partida_operacional(partida_id, competicao) or {}
        except Exception:
            partida = {}

    estado.setdefault("competicao", competicao)
    estado.setdefault("partida_id", partida_id)

    # O jogo do apontador usa a ordem operacional da partida.
    # Não deixe um estado antigo vindo do banco com equipe_a/equipe_b originais
    # sobrescrever a ordem operacional, senão o placar ao vivo atrela nome/saque
    # ao lado inicial da quadra e troca as informações quando inverte.
    equipe_a_op = partida.get("equipe_a_operacional") or estado.get("equipe_a_operacional") or partida.get("equipe_a") or estado.get("equipe_a") or ""
    equipe_b_op = partida.get("equipe_b_operacional") or estado.get("equipe_b_operacional") or partida.get("equipe_b") or estado.get("equipe_b") or ""

    estado["equipe_a_operacional"] = equipe_a_op
    estado["equipe_b_operacional"] = equipe_b_op
    estado["equipe_a"] = equipe_a_op
    estado["equipe_b"] = equipe_b_op
    estado = _aplicar_escudos_estado(estado, competicao, equipe_a_op, equipe_b_op)

    if _deve_rebuild_pesado_estado(origem=origem, estado=estado):
        estado = _reconciliar_placar_com_eventos(partida_id, competicao, estado)
        estado["evolucao_pontos"] = _montar_evolucao_pontos(
            partida_id,
            competicao,
            estado.get("set_atual") or 1,
        )
    else:
        # Durante ponto/tempo/substituição, evita consultar eventos no Neon.
        # Usa a evolução já presente no cache/retorno; se não houver, envia lista vazia.
        estado.setdefault("evolucao_pontos", estado.get("evolucao") or [])

    estado.setdefault("pontos_a", estado.get("placar_a", 0))
    estado.setdefault("pontos_b", estado.get("placar_b", 0))
    estado.setdefault("placar_a", estado.get("pontos_a", 0))
    estado.setdefault("placar_b", estado.get("pontos_b", 0))

    estado.setdefault("historico", estado.get("historico") or [])
    estado.setdefault("scout", estado.get("scout") or {})

    estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)

    apontador_login = (
        session.get("usuario")
        or estado.get("apontador")
        or estado.get("apontador_login")
        or estado.get("operador_login")
        or partida.get("operador_login")
        or ""
    )

    if apontador_login:
        estado["apontador"] = apontador_login

    try:
        atualizar_estado_cache(partida_id, estado)
        emitir_estado_partida(partida_id, estado)
        if apontador_login:
            emitir_placar_apontador(apontador_login, partida_id, estado)
    except Exception as e:
        print(f"ERRO emitir estado/placar {origem}:", e, flush=True)

    return estado


# =========================================================
# CONSULTAS BÁSICAS
# =========================================================
def listar_apontadores():
    try:
        garantir_coluna_jogo_avulso_apontador()
    except Exception as e:
        print("ERRO garantir coluna jogo avulso:", e, flush=True)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(TRIM(o.nome), ''), 'Apontador sem nome') AS nome,
                    REGEXP_REPLACE(COALESCE(a.cpf, o.cpf, ''), '\\D', '', 'g') AS cpf,
                    a.ativo,
                    a.primeiro_acesso,
                    COALESCE(a.pode_criar_jogo_avulso, FALSE) AS pode_criar_jogo_avulso
                FROM apontadores_acesso a
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') =
                     REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g')
                ORDER BY COALESCE(NULLIF(TRIM(o.nome), ''), 'Apontador sem nome')
            """)
            return cur.fetchall()


@apontadores_bp.route("/apontadores", methods=["GET", "POST"])
@exigir_perfil("superadmin")
def apontadores():
    _garantir_tabelas_oficiais_once()

    if request.method == "POST":
        nome = (request.form.get("nome") or "").strip()
        cpf_limpo = somente_digitos(request.form.get("cpf") or "")

        if not nome:
            flash("Informe o nome do apontador.", "erro")
            return redirect(url_for("apontadores.apontadores"))

        if not cpf_valido(cpf_limpo):
            flash("Informe um CPF válido para o apontador.", "erro")
            return redirect(url_for("apontadores.apontadores"))

        try:
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT cpf
                        FROM oficiais
                        WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                        LIMIT 1
                    """, (cpf_limpo,))
                    oficial_existente = cur.fetchone()

            if oficial_existente and oficial_existente.get("cpf"):
                with conectar() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE oficiais
                               SET nome = COALESCE(NULLIF(%s, ''), nome),
                                   cpf = %s
                             WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                        """, (nome, cpf_limpo, cpf_limpo))
                    conn.commit()
            else:
                cadastrar_oficial(nome, cpf_limpo)

            criar_apontador(cpf_limpo)
            flash("Apontador cadastrado com sucesso. Ele ainda não está vinculado a nenhuma competição.", "sucesso")
        except Exception as e:
            print("ERRO cadastrar apontador global:", e, flush=True)
            flash("Erro ao cadastrar apontador. Verifique se o CPF já existe ou tente novamente.", "erro")

        return redirect(url_for("apontadores.apontadores"))

    lista = listar_apontadores()

    return render_template(
        "apontadores.html",
        apontadores=lista
    )


@apontadores_bp.route("/apontadores/excluir/<cpf>", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_apontador_global_view(cpf):
    cpf_limpo = somente_digitos(cpf or "")

    try:
        excluir_apontador_global(cpf_limpo)

        if request.headers.get("X-Requested-With") == "fetch" or request.accept_mimetypes.best == "application/json":
            return _json_no_cache({
                "ok": True,
                "cpf": cpf_limpo,
                "mensagem": "Apontador excluído do sistema."
            })

        flash("Apontador excluído permanentemente do sistema.", "sucesso")
    except Exception as e:
        print("ERRO excluir_apontador_global_view:", e, flush=True)

        if request.headers.get("X-Requested-With") == "fetch" or request.accept_mimetypes.best == "application/json":
            return _json_no_cache({
                "ok": False,
                "mensagem": "Erro ao excluir apontador."
            }, 500)

        flash("Erro ao excluir apontador.", "erro")

    return redirect(url_for("apontadores.apontadores"))



@apontadores_bp.route("/apontadores/jogo-avulso/<cpf>/<acao>", methods=["POST"])
@exigir_perfil("superadmin")
def alterar_permissao_jogo_avulso_view(cpf, acao):
    liberar = str(acao or "").strip().lower() in {"liberar", "1", "true", "sim"}

    try:
        ok = definir_permissao_jogo_avulso_apontador(cpf, liberar)
        if request.headers.get("X-Requested-With") == "fetch" or request.accept_mimetypes.best == "application/json":
            return _json_no_cache({
                "ok": bool(ok),
                "liberado": bool(liberar),
                "mensagem": "Permissão atualizada." if ok else "Apontador não encontrado.",
            }, 200 if ok else 404)

        flash("Jogo rápido liberado para o apontador." if liberar else "Jogo rápido bloqueado para o apontador.", "sucesso" if ok else "erro")
    except Exception as e:
        print("ERRO alterar_permissao_jogo_avulso_view:", e, flush=True)
        flash("Erro ao alterar permissão do jogo rápido.", "erro")

    return redirect(url_for("apontadores.apontadores"))

@apontadores_bp.route("/apontador")
@exigir_perfil("apontador")
def painel_apontador():
    _garantir_tabelas_oficiais_once()

    cpf = _login_apontador_sessao()
    pode_jogo_avulso = apontador_pode_criar_jogo_avulso(cpf) if cpf else False
    offline_habilitado = offline_global_habilitado()

    if not cpf:
        flash("CPF do apontador não encontrado na sessão.", "erro")
        return render_template(
            "painel_apontador.html",
            pode_jogo_avulso=pode_jogo_avulso,
            offline_habilitado=offline_habilitado,
        )

    oficial = buscar_oficial_por_cpf(cpf)
    if not oficial:
        flash("Não foi possível localizar o apontador pelo CPF informado.", "erro")
        return render_template(
            "painel_apontador.html",
            pode_jogo_avulso=pode_jogo_avulso,
            offline_habilitado=offline_habilitado,
        )

    competicoes = listar_competicoes_apontador(cpf)

    if not competicoes:
        return render_template(
            "painel_apontador.html",
            pode_jogo_avulso=pode_jogo_avulso,
            offline_habilitado=offline_habilitado,
        )

    if len(competicoes) == 1:
        return render_template(
            "painel_apontador.html",
            competicao_unica=competicoes[0],
            pode_jogo_avulso=pode_jogo_avulso,
            offline_habilitado=offline_habilitado,
        )

    return render_template(
        "painel_apontador.html",
        competicoes=competicoes,
        pode_jogo_avulso=pode_jogo_avulso,
        offline_habilitado=offline_habilitado,
    )



def _fase_normalizada_lista(partida):
    fase_txt = str((partida or {}).get("fase") or (partida or {}).get("fase_partida") or "grupos").strip().lower()
    if fase_txt in {"grupo", "grupos", "classificatoria", "classificatória", "classificatorias", "classificatórias"}:
        return "grupos"
    if "quarta" in fase_txt:
        return "quartas"
    if "semi" in fase_txt:
        return "semifinal"
    if "terceiro" in fase_txt or ("3" in fase_txt and "lugar" in fase_txt):
        return "terceiro_lugar"
    if "final" in fase_txt:
        return "final"
    return fase_txt or "grupos"


def _resolver_modo_operacao_partida_rapido(competicao_cfg, config_avancada, partida):
    """Versão sem consulta dentro do loop da lista do apontador."""
    partida = partida or {}
    modo_padrao = str(
        partida.get("modo_operacao")
        or (competicao_cfg or {}).get("modo_operacao")
        or "simples"
    ).strip().lower()
    modo_final = modo_padrao if modo_padrao in {"simples", "avancado"} else "simples"

    try:
        fases_config = (config_avancada or {}).get("fases_config") or {}
        regras_avancadas = fases_config.get("regras_avancadas") or {}
        origem_partida = str(partida.get("origem") or "").strip()

        if origem_partida.startswith("avanco:"):
            partes = origem_partida.split(":")
            serie_id = partes[1] if len(partes) > 1 else ""
            jogo_id = partes[2] if len(partes) > 2 else ""

            regra_jogo = (regras_avancadas.get("jogos") or {}).get(f"{serie_id}:{jogo_id}") or {}
            modo_jogo = str(regra_jogo.get("modo_operacao") or "").strip().lower()
            if modo_jogo in {"simples", "avancado"}:
                return modo_jogo

            regra_serie = (regras_avancadas.get("series") or {}).get(serie_id) or {}
            modo_serie = str(regra_serie.get("modo_operacao") or "").strip().lower()
            if modo_serie in {"simples", "avancado"}:
                return modo_serie

        fase_id = _normalizar_fase_operacao(partida.get("fase"))
        regra_fase = (regras_avancadas.get("fases") or {}).get(fase_id) or {}
        modo_fase = str(regra_fase.get("modo_operacao") or "").strip().lower()
        if modo_fase in {"simples", "avancado"}:
            return modo_fase

        if fase_id == "grupos":
            grupo = str(partida.get("grupo") or "").strip().upper()
            regra_grupo = (regras_avancadas.get("grupos") or {}).get(grupo) or {}
            modo_grupo = str(regra_grupo.get("modo_operacao") or "").strip().lower()
            if modo_grupo in {"simples", "avancado"}:
                return modo_grupo
    except Exception:
        pass

    return modo_final


def _montar_partidas_painel_apontador_cache(competicao):
    """Monta lista leve da competição para a tela do apontador.

    Não carrega atletas, papeleta, eventos nem evolução ponto a ponto.
    Essa tela só precisa listar jogos e placar resumido.
    """
    competicao = str(competicao or "").strip()
    chave = ("painel_competicao", competicao, "v2")
    cached = _cache_get(chave)
    if cached is not None:
        return cached

    competicao_cfg = buscar_competicao_por_nome(competicao) or {"nome": competicao, "sets_tipo": "melhor_de_3"}

    try:
        config_avancada = buscar_configuracao_avancada_competicao(competicao) or {}
    except Exception:
        config_avancada = {}

    partidas = listar_partidas(competicao) or []

    # Mantém a normalização existente, mas só uma vez por cache curto.
    try:
        partidas = normalizar_status_partidas_apontador(partidas, competicao)
    except Exception as e:
        print("AVISO normalizar partidas apontador:", repr(e), flush=True)

    try:
        partidas = aplicar_placar_exibicao_lista(partidas, competicao_cfg)
    except Exception as e:
        print("AVISO placar exibicao lista apontador:", repr(e), flush=True)

    partidas = sorted(partidas, key=lambda x: (x.get("ordem") or 0, x.get("id") or 0))

    for p in partidas:
        try:
            p["modo_operacao_resolvido"] = _resolver_modo_operacao_partida_rapido(competicao_cfg, config_avancada, p)
        except Exception:
            p["modo_operacao_resolvido"] = "simples"
        p["permite_scout"] = str(p.get("modo_operacao_resolvido") or "simples").lower() == "avancado"
        p["fase_normalizada"] = _fase_normalizada_lista(p)

        # Garante que nada pesado vá para o HTML dessa lista.
        for campo_pesado in ("eventos", "historico", "scout", "atletas_a", "atletas_b", "papeleta_a", "papeleta_b", "evolucao_pontos"):
            if campo_pesado in p:
                p.pop(campo_pesado, None)

    payload = {
        "competicao_cfg": competicao_cfg,
        "partidas": partidas,
        "sets_max_manual": _sets_max_competicao(competicao),
        "sets_para_vencer_manual": _sets_para_vencer_competicao(competicao),
    }
    return _cache_set(chave, payload)


@apontadores_bp.route("/apontador/entrar/<competicao>")
@exigir_perfil("apontador")
def entrar_competicao_apontador(competicao):
    session["competicao_apontador"] = competicao

    # ROTA LEVE: abrir a competição no apontador não pode carregar atletas,
    # papeletas, eventos, scout nem gerar avanço. Tudo isso fica para as telas
    # específicas de pré-jogo/jogo/finalização.
    try:
        dados = _montar_partidas_painel_apontador_cache(competicao)
    except Exception as e:
        print("ERRO montar painel competição apontador:", repr(e), flush=True)
        flash("Erro ao carregar jogos da competição. Tente novamente.", "erro")
        dados = {
            "partidas": [],
            "sets_max_manual": _sets_max_competicao(competicao),
            "sets_para_vencer_manual": _sets_para_vencer_competicao(competicao),
        }

    try:
        pin_operacional = garantir_pin_operacional_apontador(competicao, _login_apontador_sessao())
    except Exception as e:
        print("ERRO garantir_pin_operacional_apontador:", e, flush=True)
        pin_operacional = None

    try:
        pode_jogo_avulso = apontador_pode_criar_jogo_avulso(_login_apontador_sessao())
    except Exception:
        pode_jogo_avulso = False

    try:
        offline_habilitado = offline_global_habilitado()
    except Exception:
        offline_habilitado = False

    return render_template(
        "painel_apontador.html",
        modo_partidas=True,
        competicao_nome=competicao,
        partidas=dados.get("partidas") or [],
        pin_operacional=pin_operacional,
        pode_jogo_avulso=pode_jogo_avulso,
        offline_habilitado=offline_habilitado,
        sets_max_manual=dados.get("sets_max_manual") or 3,
        sets_para_vencer_manual=dados.get("sets_para_vencer_manual") or 2,
    )




# =========================================================
# MODO OFFLINE DO APONTADOR
# =========================================================
def _offline_normalizar_json(valor):
    import datetime
    import decimal

    if isinstance(valor, dict):
        return {str(k): _offline_normalizar_json(v) for k, v in valor.items()}
    if isinstance(valor, (list, tuple)):
        return [_offline_normalizar_json(v) for v in valor]
    if isinstance(valor, (datetime.datetime, datetime.date, datetime.time)):
        return valor.isoformat()
    if isinstance(valor, decimal.Decimal):
        return float(valor)
    return valor


def _offline_partida_finalizada(partida):
    status = str((partida or {}).get("status") or "").strip().lower()
    return status in {"finalizada", "finalizado", "encerrada", "encerrado"}


def _offline_url_operacao(competicao, partida):
    partida = partida or {}
    partida_id = partida.get("id")
    status_op = str(partida.get("status_operacao") or "livre").lower()
    status = str(partida.get("status") or "agendada").lower()
    jogo_iniciado = (
        status in {"em andamento", "em_andamento", "ao vivo", "ao_vivo", "iniciada", "iniciado"}
        or status_op in {"em_andamento", "ao_vivo", "jogo", "iniciada", "iniciado"}
    )

    try:
        if jogo_iniciado:
            return url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id)
        return url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id)
    except Exception:
        return f"/apontador/pre-jogo/{competicao}/{partida_id}"


def _offline_coletar_equipes(partidas):
    equipes = []
    vistos = set()
    for p in partidas or []:
        for nome in (
            p.get("equipe_a"),
            p.get("equipe_b"),
            p.get("equipe_a_operacional"),
            p.get("equipe_b_operacional"),
        ):
            nome = str(nome or "").strip()
            if nome and nome not in vistos and nome.lower() != "a definir":
                vistos.add(nome)
                equipes.append({"nome": nome})
    return equipes


def _offline_coletar_atletas(competicao, equipes):
    atletas = []
    vistos = set()
    for eq in equipes or []:
        nome_equipe = (eq.get("nome") or "").strip()
        if not nome_equipe:
            continue
        try:
            lista = listar_atletas_aprovados_da_equipe(nome_equipe, competicao) or []
        except Exception as e:
            print("ERRO offline atletas:", nome_equipe, e, flush=True)
            lista = []

        for atleta in lista:
            item = dict(atleta or {})
            item["equipe"] = nome_equipe
            chave = (
                str(item.get("id") or ""),
                nome_equipe,
                str(item.get("numero") or item.get("camisa") or item.get("nome") or ""),
            )
            if chave in vistos:
                continue
            vistos.add(chave)
            atletas.append(item)
    return atletas


def _offline_coletar_papeletas(competicao, partidas):
    papeletas = []
    for p in partidas or []:
        partida_id = p.get("id")
        if not partida_id:
            continue
        equipes = [
            p.get("equipe_a_operacional") or p.get("equipe_a"),
            p.get("equipe_b_operacional") or p.get("equipe_b"),
        ]
        for equipe in equipes:
            equipe = str(equipe or "").strip()
            if not equipe or equipe.lower() == "a definir":
                continue
            for set_numero in range(1, 6):
                try:
                    dados = listar_papeleta(partida_id, competicao, equipe, set_numero) or []
                except Exception:
                    dados = []
                if dados:
                    papeletas.append({
                        "partida_id": partida_id,
                        "competicao": competicao,
                        "equipe": equipe,
                        "set_numero": set_numero,
                        "jogadores": dados,
                    })
    return papeletas


def _offline_coletar_estados_eventos(competicao, partidas):
    estados = {}
    eventos = {}
    for p in partidas or []:
        partida_id = p.get("id")
        if not partida_id:
            continue
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        except Exception:
            estado = {}
        try:
            evs = listar_eventos_partida(partida_id, competicao, limite=500) or []
        except TypeError:
            try:
                evs = listar_eventos_partida(partida_id, competicao) or []
            except Exception:
                evs = []
        except Exception:
            evs = []

        estados[str(partida_id)] = estado
        eventos[str(partida_id)] = evs
    return estados, eventos


@apontadores_bp.route("/apontador/offline/pacote/<path:competicao>")
@exigir_perfil("apontador")
def pacote_offline_competicao_apontador(competicao):
    """Pacote completo da competição para salvar no IndexedDB do dispositivo."""
    if not offline_global_habilitado():
        return _json_no_cache({
            "ok": False,
            "erro": "Modo offline bloqueado pelo Super ADM.",
        }, 403)

    try:
        comp = buscar_competicao_por_nome(competicao) or {"competicao": competicao}
        partidas_brutas = listar_partidas(competicao) or []
        partidas_brutas = sorted(partidas_brutas, key=lambda x: (x.get("ordem") or 0, x.get("id") or 0))

        partidas = []
        for p in partidas_brutas:
            item = dict(p or {})
            item["id"] = str(item.get("id") or "")
            item["competicao"] = competicao
            item["finalizada"] = _offline_partida_finalizada(item)
            item["url"] = _offline_url_operacao(competicao, item)
            partidas.append(item)

        equipes = _offline_coletar_equipes(partidas)
        atletas = _offline_coletar_atletas(competicao, equipes)
        papeletas = _offline_coletar_papeletas(competicao, partidas)
        estados, eventos = _offline_coletar_estados_eventos(competicao, partidas)

        payload = {
            "ok": True,
            "competicao": competicao,
            "baixado_em": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "usuario": _login_apontador_sessao(),
            "nome_usuario": session.get("nome") or session.get("usuario_nome") or _login_apontador_sessao() or "Apontador",
            "configuracao": comp,
            "partidas": partidas,
            "equipes": equipes,
            "atletas": atletas,
            "papeletas": papeletas,
            "estados": estados,
            "eventos": eventos,
            "resumo": {
                "total_partidas": len(partidas),
                "partidas_offline": len([p for p in partidas if not p.get("finalizada")]),
                "total_equipes": len(equipes),
                "total_atletas": len(atletas),
                "total_papeletas": len(papeletas),
            },
        }

        return _json_no_cache(_offline_normalizar_json(payload))
    except Exception as e:
        print("ERRO pacote_offline_competicao_apontador:", e, flush=True)
        return _json_no_cache({
            "ok": False,
            "erro": "Erro ao montar pacote offline da competição.",
        }, 500)


@apontadores_bp.route("/offline-apontador")
def offline_apontador_view():
    # Página simples; os dados reais são lidos do IndexedDB/localStorage no navegador.
    if not offline_global_habilitado():
        flash("Modo offline bloqueado pelo Super ADM.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    return render_template("offline_apontador.html")



@apontadores_bp.route("/apontador/resultado-manual/<competicao>/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def salvar_resultado_manual_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    sets = _coletar_sets_form_manual()
    origem = (request.form.get("origem_resultado") or "manual").strip().lower()
    if origem not in {"manual", "edicao_manual"}:
        origem = "manual"

    ok, msg = salvar_resultado_manual_partida(
        partida_id,
        competicao,
        sets,
        operador_login=_login_apontador_sessao(),
        origem=origem,
    )

    if ok:
        resultado_avanco = _atualizar_avanco_apos_finalizacao(competicao)
        novas = (resultado_avanco or {}).get("criadas", 0)
        atualizadas = (resultado_avanco or {}).get("atualizadas", 0)
        if novas or atualizadas:
            flash(f"{msg} Novo(s) jogo(s) do avanço atualizado(s).", "sucesso")
        else:
            flash(msg, "sucesso")
    else:
        flash(msg, "erro")
    return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))


@apontadores_bp.route("/apontador/scout/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def editar_scout_partida_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    modo = _resolver_modo_operacao_partida(competicao, partida)
    if modo != "avancado":
        flash("Scout disponível apenas em partidas configuradas no modo avançado.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    flash("Scout opcional: abra a partida para consultar/preencher as ações por atleta.", "sucesso")
    return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id, editar_scout="1"))


# =========================================================
# PRÉ-JOGO
# =========================================================
@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_pre_jogo_apontador(competicao, partida_id):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("equipe_a_operacional") or partida.get("equipe_b_operacional"):
        try:
            partida = aplicar_capitaes_padrao_partida(partida_id, competicao) or partida
        except Exception:
            pass

    bloqueada_por_outro = False
    if partida.get("operador_login") and partida.get("operador_login") != cpf:
        try:
            ok_lock, msg_lock, _ = validar_operador_partida(partida_id, competicao, cpf, renovar=False)
            bloqueada_por_outro = (not ok_lock) and ("operação por" in (msg_lock or "").lower())
        except Exception:
            bloqueada_por_outro = (
                partida.get("operador_login")
                and partida.get("operador_login") != cpf
                and (partida.get("status_operacao") or "livre").lower() in {"reservado", "pre_jogo", "em_andamento"}
            )

    arbitros = _listar_arbitros_competicao_cache(competicao)

    equipe_a_conferida = False
    equipe_b_conferida = False

    equipe_a_operacional = partida.get("equipe_a_operacional")
    equipe_b_operacional = partida.get("equipe_b_operacional")

    if equipe_a_operacional:
        equipe_a_conferida = equipe_ja_conferida(competicao, equipe_a_operacional)

    if equipe_b_operacional:
        equipe_b_conferida = equipe_ja_conferida(competicao, equipe_b_operacional)

    precisa_conferencia = False
    if equipe_a_operacional and equipe_b_operacional:
        precisa_conferencia = (not equipe_a_conferida) or (not equipe_b_conferida)

    fase_fluxo = str(partida.get("fase_partida") or partida.get("status_jogo") or "pre_jogo").strip().lower()
    if fase_fluxo in {"", "aguardando", "agendada", "agendado", "reservado", "livre"}:
        fase_fluxo = "pre_jogo"

    fluxo = {
        "fase_partida": fase_fluxo,
        "tiebreak_pendente": bool(partida.get("tiebreak_pendente")),
    }

    return render_template(
        "pre_jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
        arbitros=arbitros,
        bloqueada_por_outro=bloqueada_por_outro,
        equipe_a_conferida=equipe_a_conferida,
        equipe_b_conferida=equipe_b_conferida,
        precisa_conferencia=precisa_conferencia,
        capitao_a_nome=partida.get("capitao_a_nome"),
        capitao_a_numero=partida.get("capitao_a_numero"),
        capitao_b_nome=partida.get("capitao_b_nome"),
        capitao_b_numero=partida.get("capitao_b_numero"),
        pre_jogo_bloqueado=(fluxo.get("fase_partida") != "pre_jogo"),
        tie_break_pendente=bool(fluxo.get("tiebreak_pendente")),
        operador_login_atual=_login_apontador_sessao(),
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/assumir", methods=["POST"])
@exigir_perfil("apontador")
def assumir_partida_view(competicao, partida_id):
    operador_login = _login_apontador_sessao()
    cpf_sessao = (session.get("usuario") or session.get("cpf") or operador_login or "").strip()

    oficial = None
    try:
        oficial = buscar_oficial_por_cpf(cpf_sessao)
    except Exception:
        oficial = None

    operador_nome = (
        (oficial or {}).get("nome")
        or session.get("nome")
        or session.get("usuario_nome")
        or operador_login
        or "Apontador"
    )

    if not operador_login:
        flash("Sessão do apontador não identificada. Faça login novamente.", "erro")
        return redirect(url_for("login"))

    ok, msg = assumir_partida_operacional(
        partida_id,
        competicao,
        operador_login,
        operador_nome
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id, rapido="1"))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/abandonar", methods=["POST"])
@exigir_perfil("apontador")
def abandonar_partida_view(competicao, partida_id):
    cpf = _login_apontador_sessao()
    ok, msg = abandonar_partida_operacional(partida_id, competicao, cpf)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_pre_jogo_view(competicao, partida_id):
    cpf = _login_apontador_sessao()

    arbitro_1_cpf = request.form.get("arbitro_1_cpf", "").strip()
    arbitro_2_cpf = request.form.get("arbitro_2_cpf", "").strip()
    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    lado_esquerdo = request.form.get("lado_esquerdo", "").strip()
    saque_inicial = request.form.get("saque_inicial", "").strip()

    ok, msg = salvar_pre_jogo_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        arbitro_1_cpf=arbitro_1_cpf,
        arbitro_2_cpf=arbitro_2_cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_inicial=saque_inicial,
        lado_esquerdo=lado_esquerdo,
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id, rapido="1"))


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_tiebreak_view(competicao, partida_id):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer o sorteio do tie-break.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}
    if fluxo.get("fase_partida") != "tiebreak_sorteio":
        flash("O sorteio do tie-break não está liberado neste momento.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    return render_template(
        "tiebreak_sorteio_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_tiebreak_view(competicao, partida_id):
    cpf = _login_apontador_sessao()

    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    saque_tiebreak = request.form.get("saque_tiebreak", "").strip()
    lado_esquerdo_tiebreak = request.form.get("lado_esquerdo_tiebreak", "").strip()

    ok, msg = salvar_sorteio_tiebreak_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_tiebreak=saque_tiebreak,
        lado_esquerdo_tiebreak=lado_esquerdo_tiebreak,
    )

    flash(msg, "sucesso" if ok else "erro")
    if ok:
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))
    return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>")
@exigir_perfil("apontador")
def conferencia_equipe_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Salve primeiro o sorteio para definir as equipes operacionais.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = _listar_atletas_aprovados_cache(equipe, competicao)

    return render_template(
        "conferencia_equipe.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_conferencia_equipe_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode salvar a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe não definida para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    ids = [str(i).strip() for i in request.form.getlist("atleta_id") if str(i).strip()]
    atletas_atuais = listar_atletas_aprovados_da_equipe(equipe, competicao) or []
    atletas_por_id = {str(a.get("id")): a for a in atletas_atuais}

    novos_numeros = {}
    numeros_usados = {}
    erros = []

    # Valida tudo antes de salvar. Assim não aparece a mesma mensagem várias vezes
    # e também evita salvar metade da conferência quando existe número repetido.
    for atleta_id in ids:
        atleta = atletas_por_id.get(str(atleta_id))
        if not atleta:
            continue

        bruto = (request.form.get(f"numero_{atleta_id}", "") or "").strip()
        if bruto == "":
            novos_numeros[atleta_id] = None
            continue

        try:
            numero_int = int(bruto)
        except (TypeError, ValueError):
            erros.append(f"Número inválido para {atleta.get('nome') or 'atleta'}.")
            continue

        if numero_int < 1 or numero_int > 99:
            erros.append(f"O número de {atleta.get('nome') or 'atleta'} precisa ser entre 1 e 99.")
            continue

        novos_numeros[atleta_id] = numero_int
        numeros_usados.setdefault(numero_int, []).append(atleta.get("nome") or f"Atleta {atleta_id}")

    repetidos = {n: nomes for n, nomes in numeros_usados.items() if len(nomes) > 1}
    for numero, nomes in repetidos.items():
        erros.append(f"O número {numero} foi informado para mais de uma atleta: {', '.join(nomes)}.")

    if erros:
        for msg in erros:
            flash(msg, "erro")
        return redirect(url_for("apontadores.conferencia_equipe_view", competicao=competicao, partida_id=partida_id, lado=lado))

    houve_erro = False
    mensagens_exibidas = set()

    for atleta_id, numero in novos_numeros.items():
        atleta = atletas_por_id.get(str(atleta_id)) or {}
        numero_atual = atleta.get("numero")
        try:
            numero_atual = int(numero_atual) if numero_atual not in (None, "") else None
        except (TypeError, ValueError):
            numero_atual = None

        # Se não mudou, não chama o banco. Isso evita falso erro quando a tela
        # envia vários campos de uma vez e deixa o salvamento bem mais leve.
        if numero_atual == numero:
            continue

        ok, msg = atualizar_numero_atleta(atleta_id, "" if numero is None else str(numero))
        if not ok:
            houve_erro = True
            if msg not in mensagens_exibidas:
                mensagens_exibidas.add(msg)
                flash(msg, "erro")

    if not houve_erro:
        libero_ids = [str(i).strip() for i in request.form.getlist("libero_id") if str(i).strip()]
        ok_libero, msg_libero = salvar_liberos_equipe(equipe, competicao, libero_ids)
        if not ok_libero:
            houve_erro = True
            flash(msg_libero, "erro")
            return redirect(url_for("apontadores.conferencia_equipe_view", competicao=competicao, partida_id=partida_id, lado=lado))

        _limpar_cache_atletas(equipe, competicao)
        marcar_equipe_conferida(competicao, equipe)
        flash("Conferência salva com sucesso.", "sucesso")

    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>/atleta/<int:atleta_id>/editar", methods=["POST"])
@exigir_perfil("apontador")
def editar_atleta_conferencia_view(competicao, partida_id, lado, atleta_id):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode editar atletas na conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe não definida para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    nome = request.form.get("nome", "")
    cpf_atleta = request.form.get("cpf", "")
    data_nascimento = request.form.get("data_nascimento", "")
    numero = request.form.get("numero", "")
    libero = request.form.get("libero") == "1"

    ok, msg = atualizar_atleta_conferencia_apontador(
        atleta_id, equipe, competicao, nome, cpf_atleta, data_nascimento, numero=numero, libero=libero
    )

    _limpar_cache_atletas(equipe, competicao)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.conferencia_equipe_view", competicao=competicao, partida_id=partida_id, lado=lado))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>")
@exigir_perfil("apontador")
def definir_capitao_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode definir o capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe operacional ainda não definida.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = _listar_atletas_aprovados_cache(equipe, competicao)
    atletas = [a for a in atletas if a.get("numero") not in (None, "")]
    atleta_atual_id = partida.get("capitao_a_id") if lado == "A" else partida.get("capitao_b_id")

    return render_template(
        "definir_capitao.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
        atleta_atual_id=atleta_atual_id,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_capitao_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    atleta_id = request.form.get("atleta_id", "").strip()

    ok, msg = salvar_capitao_partida(partida_id, competicao, cpf, lado, atleta_id)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


# =========================================================
# PAPELETA
# =========================================================
@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    fase = (partida.get("fase_partida") or "papeleta").strip().lower()

    if fase == "encerrado":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if fase == "pre_jogo":
        flash("Finalize primeiro o pré-jogo para acessar a papeleta.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    if fase == "tiebreak_sorteio":
        flash("Antes do tie-break, faça o sorteio específico do set decisivo.", "erro")
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    if fase == "jogo":
        return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))

    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id, competicao, partida
    )

    atletas_a = _listar_atletas_aprovados_cache(equipe_a, competicao) if equipe_a else []
    atletas_b = _listar_atletas_aprovados_cache(equipe_b, competicao) if equipe_b else []

    atletas_a = [a for a in atletas_a if a.get("numero")]
    atletas_b = [a for a in atletas_b if a.get("numero")]

    fluxo = {
        "fase_partida": fase,
        "papeleta_a_completa": all(papeleta_a.get(i) for i in range(1, 7)),
        "papeleta_b_completa": all(papeleta_b.get(i) for i in range(1, 7)),
        "set_atual": set_atual,
    }

    return render_template(
        "papeleta_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        equipe_a=equipe_a,
        equipe_b=equipe_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def salvar_papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    equipe_a = partida.get("equipe_a_operacional") or partida.get("equipe_a")
    equipe_b = partida.get("equipe_b_operacional") or partida.get("equipe_b")
    set_atual = int(partida.get("set_atual") or 1)

    atletas_cache = {}

    def montar_dados(lado, equipe):
        if not equipe:
            return {}

        atletas = atletas_cache.get(equipe)
        if atletas is None:
            atletas = _listar_atletas_aprovados_cache(equipe, competicao)
            atletas_cache[equipe] = atletas

        mapa = {
            int(a.get("numero")): a
            for a in atletas
            if a.get("numero") not in (None, "")
        }

        dados = {}

        for pos in [1, 2, 3, 4, 5, 6]:
            valor = (request.form.get(f"{lado}_{pos}") or "").strip()
            if not valor:
                continue

            try:
                numero = int(valor)
            except Exception:
                continue

            atleta = mapa.get(numero)
            if atleta:
                dados[pos] = atleta

        return dados

    dados_a = montar_dados("A", equipe_a)
    dados_b = montar_dados("B", equipe_b)

    if len(dados_a) != 6 or len(dados_b) != 6:
        flash("Preencha as 6 posições das duas equipes.", "erro")
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    salvar_papeleta(partida_id, competicao, equipe_a, set_atual, dados_a)
    salvar_papeleta(partida_id, competicao, equipe_b, set_atual, dados_b)

    rotacao_a = [
        str(dados_a[4].get("numero") or ""),
        str(dados_a[3].get("numero") or ""),
        str(dados_a[2].get("numero") or ""),
        str(dados_a[5].get("numero") or ""),
        str(dados_a[6].get("numero") or ""),
        str(dados_a[1].get("numero") or ""),
    ]

    rotacao_b = [
        str(dados_b[4].get("numero") or ""),
        str(dados_b[3].get("numero") or ""),
        str(dados_b[2].get("numero") or ""),
        str(dados_b[5].get("numero") or ""),
        str(dados_b[6].get("numero") or ""),
        str(dados_b[1].get("numero") or ""),
    ]

    try:
        inicializar_jogo_partida(partida_id, competicao)
    except Exception as e:
        print("ERRO inicializar_jogo_partida:", repr(e), flush=True)

    estado = {
        "ok": True,
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": equipe_a or "",
        "equipe_b": equipe_b or "",
        "pontos_a": int(partida.get("pontos_a") or 0),
        "pontos_b": int(partida.get("pontos_b") or 0),
        "placar_a": int(partida.get("pontos_a") or 0),
        "placar_b": int(partida.get("pontos_b") or 0),
        "sets_a": int(partida.get("sets_a") or 0),
        "sets_b": int(partida.get("sets_b") or 0),
        "set_atual": set_atual,
        "saque_atual": partida.get("saque_atual") or partida.get("saque_inicial") or "",
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "historico": [{"descricao": "Jogo iniciado"}],
        "ultima_acao": "Jogo iniciado",
        "fase_partida": "jogo",
        "status_jogo": "em_andamento",
    }

    _emitir_estado_e_placar(partida_id, competicao, estado, partida=partida, origem="PAPELETA")

    flash("Papeleta salva com sucesso.", "sucesso")
    return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))



@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/heartbeat", methods=["POST"])
@exigir_perfil("apontador")
def heartbeat_partida_view(competicao, partida_id):
    login = _login_apontador_sessao()
    ok, msg = heartbeat_partida_operacional(partida_id, competicao, login)
    return _json_no_cache({"ok": ok, "mensagem": msg, "bloqueada": not ok}, 200 if ok else 423)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/liberar", methods=["POST"])
@exigir_perfil("apontador")
def liberar_partida_operacional_view(competicao, partida_id):
    login = _login_apontador_sessao()
    ok, msg = liberar_trava_partida_operacional(partida_id, competicao, login)
    return _json_no_cache({"ok": ok, "mensagem": msg}, 200 if ok else 400)


# =========================================================
# JOGO
# =========================================================
@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def jogo_view(competicao, partida_id):
    """
    Abre/retoma a partida de forma rápida.

    IMPORTANTE:
    Antes esta rota reconstruía histórico, evolução de pontos e placar lendo a
    tabela eventos antes de renderizar. Em partidas pausadas isso podia travar
    por minutos. Agora a tela abre com o estado salvo/cacheado e o JS/socket
    continua a sincronização depois, sem bloquear o clique em "Retomar partida".
    """
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    ok_lock, msg_lock, partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
    if not ok_lock:
        flash(msg_lock, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))
    if partida_lock:
        partida = partida_lock

    status_jogo = (partida.get("status_jogo") or "").strip().lower()
    status_operacao = (partida.get("status_operacao") or "").strip().lower()

    editar_scout_finalizada = request.args.get("editar_scout") == "1"
    if status_jogo in {"finalizada", "finalizado", "encerrada", "encerrado"}:
        modo_finalizada = _resolver_modo_operacao_partida(competicao, partida)
        if not (editar_scout_finalizada and modo_finalizada == "avancado"):
            flash("A partida já está finalizada.", "erro")
            return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    # Não reinicializa partida pausada. Apenas abre do ponto salvo.
    if (not editar_scout_finalizada) and status_jogo not in {"em_andamento", "entre_sets", "pausada", "pausado"} and status_operacao not in {"pausada", "pausado"}:
        try:
            partida = inicializar_jogo_partida(partida_id, competicao) or partida
        except Exception as e:
            print("ERRO inicializar_jogo_partida/jogo_view rapido:", repr(e), flush=True)

    # Prioridade para cache vivo; se não existir, usa snapshot salvo no banco.
    try:
        estado = obter_estado_cache(partida_id) or {}
    except Exception:
        estado = {}

    if not estado:
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        except Exception as e:
            print("ERRO buscar_estado_jogo_partida/jogo_view rapido:", repr(e), flush=True)
            estado = {}

    estado = dict(estado or {})

    # Montagem mínima e segura do estado, sem varrer eventos.
    estado.setdefault("ok", True)
    estado["competicao"] = competicao
    estado["partida_id"] = partida_id

    equipe_a_op = partida.get("equipe_a_operacional") or partida.get("equipe_a") or estado.get("equipe_a") or ""
    equipe_b_op = partida.get("equipe_b_operacional") or partida.get("equipe_b") or estado.get("equipe_b") or ""

    if (not equipe_a_op or not equipe_b_op) and not editar_scout_finalizada:
        flash("Complete o pré-jogo antes de abrir a tela do jogo.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    estado["equipe_a_operacional"] = equipe_a_op
    estado["equipe_b_operacional"] = equipe_b_op
    estado["equipe_a"] = equipe_a_op
    estado["equipe_b"] = equipe_b_op
    estado = _aplicar_escudos_estado(estado, competicao, equipe_a_op, equipe_b_op)

    for campo, padrao in (
        ("pontos_a", partida.get("pontos_a") or 0),
        ("pontos_b", partida.get("pontos_b") or 0),
        ("sets_a", partida.get("sets_a") or 0),
        ("sets_b", partida.get("sets_b") or 0),
        ("set_atual", partida.get("set_atual") or 1),
    ):
        if estado.get(campo) in (None, ""):
            estado[campo] = padrao

    estado["placar_a"] = estado.get("placar_a", estado.get("pontos_a", 0))
    estado["placar_b"] = estado.get("placar_b", estado.get("pontos_b", 0))
    estado["saque_atual"] = estado.get("saque_atual") or partida.get("saque_atual") or partida.get("saque_inicial") or ""
    estado.setdefault("status_jogo", status_jogo or "em_andamento")
    estado.setdefault("fase_partida", partida.get("fase_partida") or "jogo")

    # Papeleta e atletas são necessários para a operação da tela, mas são leves
    # comparados à reconstrução por eventos. Mantemos com fallback seguro.
    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id,
        competicao,
        partida,
        estado,
    )

    try:
        # Sempre busca o elenco direto no banco ao abrir/retomar a partida.
        # O cache antigo deixava a tela sem atletas quando os números eram cadastrados
        # depois que o apontador já tinha aberto alguma tela da competição.
        atletas_a = listar_atletas_aprovados_da_equipe(equipe_a, competicao) if equipe_a else []
        atletas_b = listar_atletas_aprovados_da_equipe(equipe_b, competicao) if equipe_b else []
    except Exception as e:
        print("ERRO atletas jogo_view rapido:", repr(e), flush=True)
        atletas_a = []
        atletas_b = []

    if not _rotacao_tem_atletas_front(estado.get("rotacao_a")):
        estado["rotacao_a"] = _rotacao_fallback_por_papeleta(papeleta_a)

    if not _rotacao_tem_atletas_front(estado.get("rotacao_b")):
        estado["rotacao_b"] = _rotacao_fallback_por_papeleta(papeleta_b)

    # Garante que os modais tenham números mesmo se o SELECT de atletas vier vazio
    # ou com numeração em campo antigo. Usa papeleta/rotação já carregadas.
    atletas_a = _merge_atletas_operacionais(atletas_a, papeleta_a, estado.get("rotacao_a"))
    atletas_b = _merge_atletas_operacionais(atletas_b, papeleta_b, estado.get("rotacao_b"))

    estado["rotacao"] = {
        "equipe_a": estado.get("rotacao_a") or ["", "", "", "", "", ""],
        "equipe_b": estado.get("rotacao_b") or ["", "", "", "", "", ""],
    }

    # Não busca histórico/eventos aqui. Isso destravava o "Retomar partida".
    estado.setdefault("historico", [])
    estado.setdefault("ultima_acao", estado.get("ultima_acao") or "Partida retomada")
    estado.setdefault("evolucao_pontos", estado.get("evolucao_pontos") or [])
    estado.setdefault("scout", estado.get("scout") or {})

    estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)

    try:
        competicao_cfg_estado = buscar_competicao_por_nome(competicao) or {"nome": competicao, "sets_tipo": partida.get("sets_tipo") or "melhor_de_3"}
        estado = aplicar_placar_exibicao_partida(dict(estado or {}), competicao_cfg_estado)
    except Exception as e:
        print("AVISO aplicar placar exibicao jogo_view:", repr(e), flush=True)

    try:
        atualizar_estado_cache(partida_id, estado)
    except Exception as e:
        print("AVISO atualizar cache jogo_view rapido:", repr(e), flush=True)

    resposta = make_response(render_template(
        "jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        estado=estado,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        modo_operacao=_resolver_modo_operacao_partida(competicao, partida),
        offline_habilitado=offline_global_habilitado(),
    ))

    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/ponto", methods=["POST"])
@exigir_perfil("apontador")
def ponto_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        fundamento = (request.form.get("fundamento") or corpo.get("fundamento") or "").strip().lower()
        resultado = (request.form.get("resultado") or corpo.get("resultado") or "").strip().lower()
        tipo_lance = (request.form.get("tipo_lance") or corpo.get("tipo_lance") or "").strip().lower()
        detalhe_lance = (request.form.get("detalhe_lance") or corpo.get("detalhe_lance") or "").strip().lower()
        tipo_erro = (request.form.get("tipo_erro") or corpo.get("tipo_erro") or "").strip().lower()
        atleta_numero = str(request.form.get("atleta_numero") or corpo.get("atleta_numero") or "").strip()
        atleta_nome = (request.form.get("atleta_nome") or corpo.get("atleta_nome") or "").strip()
        atleta_label = (request.form.get("atleta_label") or corpo.get("atleta_label") or "").strip()

        if not tipo_lance:
            return _json_no_cache({"ok": False, "mensagem": "Selecione se foi ponto, erro ou falta."}, 400)

        # O modo simples do apontador/mobile envia ponto_simples.
        # Ele deve contar como ponto direto, sem obrigar scout/atleta.
        if tipo_lance == "ponto_simples":
            tipo_lance = "ponto"
            resultado = "ponto"
            detalhe_lance = detalhe_lance or "ponto_simples"

        if tipo_lance not in {"ponto", "erro", "falta"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de lance inválido."}, 400)

        detalhe_final = (detalhe_lance or tipo_erro or resultado or fundamento).strip().lower()

        detalhes_validos = {
            "ponto": {"ataque", "bloqueio", "ace", "ponto_simples"},
            "erro": {"erro_saque", "erro_geral"},
            "falta": {"rede", "invasao", "rotacao", "conducao", "dois_toques"},
        }

        if detalhe_final not in detalhes_validos[tipo_lance]:
            return _json_no_cache({"ok": False, "mensagem": "Detalhe da jogada inválido."}, 400)

        exige_atleta = detalhe_final in {"ataque", "bloqueio", "ace"}

        if exige_atleta and not atleta_numero:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o atleta da jogada."}, 400)

        if not exige_atleta:
            atleta_numero = ""
            atleta_nome = ""
            atleta_label = ""

        def _lado_oposto(lado):
            return "B" if lado == "A" else "A"

        # REGRA CORRETA DO SCOUT:
        # - Quando for ponto direto, o botão clicado é quem pontua.
        # - Quando for erro/falta, o botão clicado é quem COMETEU o erro/falta,
        #   então o ponto vai automaticamente para o lado adversário.
        responsavel_lado = (
            request.form.get("responsavel_lado")
            or corpo.get("responsavel_lado")
            or ""
        ).strip().upper()

        if tipo_lance in {"erro", "falta"}:
            equipe_scout = responsavel_lado if responsavel_lado in {"A", "B"} else equipe
            equipe_pontuadora = _lado_oposto(equipe_scout)
        else:
            equipe_pontuadora = equipe
            equipe_scout = equipe_pontuadora

        detalhes_evento = {
            "fundamento": detalhe_final,
            "resultado": tipo_lance,
            "tipo_lance": tipo_lance,
            "detalhe_lance": detalhe_final,
            "tipo_erro": tipo_erro,
            "atleta_numero": atleta_numero,
            "atleta_nome": atleta_nome,
            "atleta_label": atleta_label,
            "equipe_pontuadora": equipe_pontuadora,
            "equipe_scout": equipe_scout,
            "responsavel_lado": equipe_scout,
        }

        ok, retorno = registrar_ponto_partida(
            partida_id=partida_id,
            competicao=competicao,
            equipe=equipe_pontuadora,
            tipo="ponto",
            detalhes=detalhes_evento
        )

        if not ok:
            mensagem = retorno if isinstance(retorno, str) else "Não foi possível registrar o ponto."
            return _json_no_cache({"ok": False, "mensagem": mensagem}, 400)

        estado = retorno if isinstance(retorno, dict) else {}
        estado["competicao"] = competicao
        estado["partida_id"] = partida_id
        if not estado.get("historico") or not estado.get("ultima_acao"):
            desc = "Ponto registrado"
            if atleta_label:
                desc = f"Ponto {equipe_pontuadora} • {atleta_label}"
            estado["historico"] = [{"descricao": desc}]
            estado["ultima_acao"] = desc

        # Não reconstruir histórico/evolução pelo banco a cada ponto.
        # O estado retornado por registrar_ponto_partida + socket é a fonte viva.
        estado["_forcar_rebuild_eventos"] = False

        estado = _emitir_estado_e_placar(
            partida_id,
            competicao,
            estado=estado,
            origem="PONTO_OFICIAL"
        )

        return _json_no_cache({
            "ok": True,
            "mensagem": "Ponto registrado com sucesso.",
            **estado
        })

    except Exception as e:
        print("ERRO ponto_view:", e, flush=True)
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar ponto: {e}"
        }, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/wo", methods=["POST"])
@exigir_perfil("apontador")
def wo_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}

        equipe_vencedora = (
            request.form.get("equipe_vencedora")
            or corpo.get("equipe_vencedora")
            or ""
        ).strip().upper()

        if equipe_vencedora not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_wo_partida(
            partida_id=partida_id,
            competicao=competicao,
            vencedor_lado=equipe_vencedora
        )

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        estado = _emitir_estado_e_placar(
            partida_id,
            competicao,
            estado,
            origem="WO"
        )

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida encerrada por WO.",
            **estado
        })

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar WO: {e}"
        }, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/desfazer", methods=["POST"])
@exigir_perfil("apontador")
def desfazer_acao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        ok, retorno = desfazer_ultima_acao_partida(partida_id, competicao)

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        estado["desfazer"] = True

        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="DESFAZER")

        return _json_no_cache({
            "ok": True,
            **estado
        })

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao desfazer ação: {e}"
        }, 500)


def _descricao_acao(tipo, equipe='', payload=None):
    payload = payload or {}
    equipe_txt = f"Equipe {equipe}" if equipe else "Equipe"
    if tipo == "tempo":
        return f"Tempo solicitado - {equipe_txt}"
    if tipo == "substituicao":
        return f"{equipe_txt} • substituição • {payload.get('numero_sai', '')}>{payload.get('numero_entra', '')}"
    if tipo == "substituicao_excepcional":
        return f"{equipe_txt} • substituição excepcional • {payload.get('numero_sai', '')}>{payload.get('numero_entra', '')}"
    if tipo == "retardamento":
        return f"{equipe_txt} • retardamento"
    if tipo == "sancao":
        return f"{equipe_txt} • sanção • {payload.get('tipo_sancao') or payload.get('sancao') or ''}"
    if tipo == "cartao_verde":
        return f"{equipe_txt} • cartão verde"
    return payload.get('descricao') or "Ação registrada"


def _normalizar_estado_pos_acao(partida_id, competicao, retorno=None, origem="", acao=None):
    estado = retorno if isinstance(retorno, dict) else {}
    cache = obter_estado_cache(partida_id) or {}

    # Primeiro preserva o estado que já está na tela/cache para não zerar placar/rotação.
    base = dict(cache)
    base.update(estado)
    estado = base

    historico = estado.get("historico") or cache.get("historico") or []
    if not isinstance(historico, list):
        historico = []

    if acao:
        desc = acao.get("descricao") if isinstance(acao, dict) else str(acao)
        if desc and not (historico and isinstance(historico[0], dict) and historico[0].get("descricao") == desc):
            historico.insert(0, {"descricao": desc})

    if not historico and estado.get("ultima_acao"):
        historico = [{"descricao": estado.get("ultima_acao")}]

    estado["historico"] = historico[:5]
    estado["ultima_acao"] = estado.get("ultima_acao") or (
        estado["historico"][0].get("descricao") if estado["historico"] and isinstance(estado["historico"][0], dict) else "-"
    )

    # Não buscar partida no banco a cada ação. Esse era outro gargalo do apontador.
    # O estado/cache já carrega nomes, placar, rotação e regras necessários para socket.
    return _emitir_estado_e_placar(partida_id, competicao, estado, partida={}, origem=origem)


def _salvar_async(nome, funcao, *args, **kwargs):
    def executar():
        try:
            ok, retorno = funcao(*args, **kwargs)
            if not ok:
                print(f"ERRO async {nome}: {retorno}", flush=True)
        except Exception as e:
            print(f"ERRO async {nome}:", repr(e), flush=True)

    threading.Thread(target=executar, daemon=True).start()


def _acao_rapida(partida_id, competicao, tipo, equipe='', payload=None):
    payload = payload or {}
    equipe = (equipe or '').strip().upper()
    estado = dict(obter_estado_cache(partida_id) or {})

    if not estado:
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        except Exception:
            estado = {}

    descricao = payload.get("descricao") or _descricao_acao(tipo, equipe, payload)
    historico = estado.get("historico") or []
    if not isinstance(historico, list):
        historico = []
    historico.insert(0, {"descricao": descricao})
    estado["historico"] = historico[:5]
    estado["ultima_acao"] = descricao

    # Atualizações visuais imediatas sem depender do banco.
    if tipo == "tempo":
        campo = "tempos_a" if equipe == "A" else "tempos_b"
        try:
            estado[campo] = max(0, int(estado.get(campo, 0)) + 1)
        except Exception:
            estado[campo] = 1
    elif tipo == "substituicao":
        campo = "subs_a" if equipe == "A" else "subs_b"
        try:
            estado[campo] = int(estado.get(campo, 0)) + 1
        except Exception:
            estado[campo] = 1
        numero_sai = str(payload.get("numero_sai") or '').strip()
        numero_entra = str(payload.get("numero_entra") or '').strip()
        rot_key = "rotacao_a" if equipe == "A" else "rotacao_b"
        rot = list(estado.get(rot_key) or [])
        if numero_sai and numero_entra and len(rot) == 6:
            estado[rot_key] = [numero_entra if str(n) == numero_sai else n for n in rot]

        # Marca visual para árbitros/mesa: quem saiu fica vermelho no histórico/status,
        # e quem entrou fica identificado como substituto.
        status_key = "status_jogadores_a" if equipe == "A" else "status_jogadores_b"
        status = estado.get(status_key) if isinstance(estado.get(status_key), dict) else {}
        if numero_sai:
            status[numero_sai] = {"tipo": "substituido", "numero_entra": numero_entra}
        if numero_entra:
            status[numero_entra] = {"tipo": "substituto", "numero_sai": numero_sai}
        estado[status_key] = status
    elif tipo == "cartao_verde":
        campo = "cartoes_verdes_a" if equipe == "A" else "cartoes_verdes_b"
        lista = estado.get(campo) or []
        if not isinstance(lista, list):
            lista = []
        lista.append({"tipo_pessoa": payload.get("tipo_pessoa"), "numero": payload.get("numero"), "nome": payload.get("nome")})
        estado[campo] = lista
    elif tipo == "sancao":
        campo = "sancoes_a" if equipe == "A" else "sancoes_b"
        lista = estado.get(campo) or []
        if not isinstance(lista, list):
            lista = []
        lista.append({"tipo_pessoa": payload.get("tipo_pessoa"), "numero": payload.get("numero"), "nome": payload.get("nome"), "tipo_sancao": payload.get("tipo_sancao")})
        estado[campo] = lista

    return _normalizar_estado_pos_acao(partida_id, competicao, estado, origem=f"{tipo.upper()}_RAPIDO", acao={"descricao": descricao})


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/tempo", methods=["POST"])
@exigir_perfil("apontador")
def registrar_tempo_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        # =========================
        # ⚡ ESTADO ATUAL (SEM BANCO PESADO)
        # =========================
        estado_atual = dict(obter_estado_cache(partida_id) or {})
        if not estado_atual:
            estado_atual = buscar_estado_jogo_partida(partida_id, competicao) or {}

        estado_atual = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado_atual, {})

        usado = _int_seguro(
            estado_atual.get("tempos_a") if equipe == "A" else estado_atual.get("tempos_b"), 0
        )
        limite = _int_seguro(estado_atual.get("limite_tempos"), 2)

        if usado >= limite:
            return _json_no_cache({
                "ok": False,
                "mensagem": f"Limite de tempos atingido para a Equipe {equipe} neste set.",
                **estado_atual
            }, 400)

        # =========================
        # ⚡ ATUALIZA NA HORA (SEM ESPERAR)
        # =========================
        estado = _acao_rapida(
            partida_id,
            competicao,
            "tempo",
            equipe,
            {
                "descricao": f"Tempo solicitado - Equipe {equipe}"
            }
        )

        equipe_nome = estado.get("equipe_a") if equipe == "A" else estado.get("equipe_b")

        payload = {
            "partida_id": partida_id,
            "competicao": competicao,
            "tipo": "tempo",
            "status": "iniciado",
            "duracao": 30,
            "equipe": equipe,
            "equipe_nome": equipe_nome,
            "mensagem": f"Tempo - {equipe_nome}",
            "origem": "apontador",
            "timestamp": time.time()
        }

        # =========================
        # 🚀 SOCKET IMEDIATO (SEM DELAY)
        # =========================
        try:
            # cronômetro
            emitir_tempo_executado(partida_id, payload)

            # 🔥 FORÇA ATUALIZAÇÃO EM TODAS TELAS (inclui celular)
            emitir_estado_partida(partida_id, estado)

            # 🔥 Garante que celular receba como notificação também
            socketio.emit("cronometro_arbitros", payload, room=str(partida_id))
            socketio.emit("notificacao_geral", payload, room=str(partida_id))

        except Exception as e:
            print("ERRO socket tempo:", e, flush=True)

        # =========================
        # 💾 BANCO EM BACKGROUND (SEM TRAVAR)
        # =========================
        _salvar_async(
            "tempo",
            registrar_tempo_partida,
            partida_id,
            competicao,
            equipe
        )

        return _json_no_cache({
            "ok": True,
            "mensagem": "Tempo registrado.",
            **estado
        })

    except Exception as e:
        print("ERRO registrar_tempo_view:", e, flush=True)
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar tempo: {e}"
        }, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        estado_atual = dict(obter_estado_cache(partida_id) or {})
        if not estado_atual:
            estado_atual = buscar_estado_jogo_partida(partida_id, competicao) or {}

        estado_atual = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado_atual, {})
        usado = _int_seguro(estado_atual.get("subs_a") if equipe == "A" else estado_atual.get("subs_b"), 0)
        limite = _int_seguro(estado_atual.get("limite_substituicoes"), 6)

        if usado >= limite:
            return _json_no_cache({"ok": False, "mensagem": f"Limite de substituições atingido para a Equipe {equipe} neste set.", **estado_atual}, 400)

        estado = _acao_rapida(
            partida_id,
            competicao,
            "substituicao",
            equipe,
            {"numero_sai": numero_sai, "numero_entra": numero_entra}
        )

        emitir_substituicao_executada(partida_id, {
            "equipe": equipe,
            "equipe_nome": estado.get("equipe_a") if equipe == "A" else estado.get("equipe_b"),
            "numero_sai": numero_sai,
            "numero_entra": numero_entra,
            "mensagem": f"Substituição executada - Equipe {equipe}: #{numero_sai} → #{numero_entra}",
            "origem": "apontador",
        })

        _salvar_async("substituicao", registrar_substituicao_partida, partida_id, competicao, equipe, numero_sai, numero_entra)

        return _json_no_cache({"ok": True, "mensagem": "Substituição registrada.", **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao-excepcional", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_excepcional_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()
        motivo = str(request.form.get("motivo") or corpo.get("motivo") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        try:
            ok, retorno = registrar_substituicao_excepcional_partida(
                partida_id, competicao, equipe, numero_sai, numero_entra, motivo
            )
        except TypeError:
            ok, retorno = registrar_substituicao_excepcional_partida(
                partida_id, competicao, equipe, numero_sai, numero_entra
            )

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="SUBSTITUICAO_EXCEPCIONAL")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição excepcional: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/retardamento", methods=["POST"])
@exigir_perfil("apontador")
def registrar_retardamento_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="RETARDAMENTO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar retardamento: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sancao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_sancao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()
        sancao = (request.form.get("sancao") or corpo.get("sancao") or "").strip().lower()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo da sanção."}, 400)

        if not sancao:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o tipo de sanção."}, 400)

        ok, retorno = registrar_sancao_partida(partida_id, competicao, equipe, tipo_pessoa, alvo, sancao)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="SANCAO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar sanção: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/cartao-verde", methods=["POST"])
@exigir_perfil("apontador")
def registrar_cartao_verde_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo do cartão verde."}, 400)

        ok, retorno = registrar_cartao_verde_partida(partida_id, competicao, equipe, tipo_pessoa, alvo)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="CARTAO_VERDE")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar cartão verde: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/salvar-estado", methods=["POST"])
@exigir_perfil("apontador")
def salvar_estado_manual_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        estado_recebido = corpo.get("estado") if isinstance(corpo.get("estado"), dict) else corpo
        pausar = bool(corpo.get("pausar") or corpo.get("salvar_e_sair"))

        estado_atual = obter_estado_cache(partida_id) or {}
        estado = {**estado_atual, **(estado_recebido or {})}
        estado["competicao"] = competicao
        estado["partida_id"] = partida_id
        estado_salvo = salvar_estado_manual_partida(
            partida_id=partida_id,
            competicao=competicao,
            estado=estado,
            operador=_login_apontador_sessao(),
            pausar=pausar,
        ) or estado

        atualizar_estado_cache(partida_id, estado_salvo)
        estado_salvo = _emitir_estado_e_placar(
            partida_id,
            competicao,
            estado=estado_salvo,
            origem="SALVAR_MANUAL_APONTADOR",
        )

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida pausada e salva." if pausar else "Partida salva no banco.",
            "pausada": pausar,
            **estado_salvo,
        })

    except Exception as e:
        print("ERRO salvar_estado_manual_view:", e, flush=True)
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao salvar partida: {e}",
        }, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sincronizar", methods=["POST"])
@exigir_perfil("apontador")
def sincronizar_acao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        # Offline-first oficial: sincronização intermediária não grava banco.
        # A fila completa só é persistida em /encerrar.
        estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
        return _json_no_cache({
            "ok": True,
            "ignorado": True,
            "mensagem": "Sincronização intermediária desativada. Banco salva somente ao finalizar a partida.",
            **estado_atual
        }, 200)

        corpo = request.get_json(silent=True) or {}
        tipo = (corpo.get("tipo") or "").strip().lower()
        equipe = (corpo.get("equipe") or "").strip().upper()

        if not tipo:
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            estado_atual = _emitir_estado_e_placar(
                partida_id,
                competicao,
                estado_atual,
                origem="SINCRONIZAR_SEM_TIPO"
            )
            return _json_no_cache({
                "ok": True,
                "ignorado": True,
                "mensagem": "Sincronização sem tipo ignorada.",
                **estado_atual
            }, 200)

        if tipo == "ponto":
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            estado_atual = _emitir_estado_e_placar(
                partida_id,
                competicao,
                estado_atual,
                origem="SINCRONIZAR_PONTO_IGNORADO"
            )
            return _json_no_cache({
                "ok": True,
                "ignorado": True,
                "mensagem": "Ponto já é registrado pela rota oficial.",
                **estado_atual
            }, 200)

        if tipo in {"tempo", "substituicao", "substituicao_excepcional", "retardamento", "sancao", "cartao_verde"}:
            if equipe not in {"A", "B"}:
                estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    origem="SINCRONIZAR_EQUIPE_INVALIDA"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": "Ação sem equipe válida ignorada.",
                    **estado_atual
                }, 200)

        if tipo == "tempo":
            partida = buscar_partida_operacional(partida_id, competicao) or {}
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}

            permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
                partida_id,
                competicao,
                equipe,
                "tempo",
                partida=partida,
                estado=estado_atual
            )

            if not permitido:
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    partida=partida,
                    origem="SINCRONIZAR_TEMPO_LIMITE"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": mensagem_limite,
                    **estado_atual
                }, 200)

            ok, retorno = registrar_tempo_partida(partida_id, competicao, equipe)

        elif tipo == "substituicao":
            numero_sai = str(corpo.get("numero_sai") or "").strip()
            numero_entra = str(corpo.get("numero_entra") or "").strip()

            if not numero_sai or not numero_entra:
                estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    origem="SINCRONIZAR_SUB_INVALIDA"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": "Substituição incompleta ignorada.",
                    **estado_atual
                }, 200)

            partida = buscar_partida_operacional(partida_id, competicao) or {}
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}

            permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
                partida_id,
                competicao,
                equipe,
                "substituicao",
                partida=partida,
                estado=estado_atual
            )

            if not permitido:
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    partida=partida,
                    origem="SINCRONIZAR_SUBSTITUICAO_LIMITE"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": mensagem_limite,
                    **estado_atual
                }, 200)

            ok, retorno = registrar_substituicao_partida(
                partida_id,
                competicao,
                equipe,
                numero_sai,
                numero_entra
            )

        elif tipo == "substituicao_excepcional":
            numero_sai = str(corpo.get("numero_sai") or "").strip()
            numero_entra = str(corpo.get("numero_entra") or "").strip()
            motivo = str(corpo.get("motivo") or "").strip()

            if not numero_sai or not numero_entra:
                estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    origem="SINCRONIZAR_SUB_EX_INVALIDA"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": "Substituição excepcional incompleta ignorada.",
                    **estado_atual
                }, 200)

            try:
                ok, retorno = registrar_substituicao_excepcional_partida(
                    partida_id,
                    competicao,
                    equipe,
                    numero_sai,
                    numero_entra,
                    motivo
                )
            except TypeError:
                ok, retorno = registrar_substituicao_excepcional_partida(
                    partida_id,
                    competicao,
                    equipe,
                    numero_sai,
                    numero_entra
                )

        elif tipo == "retardamento":
            ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)

        elif tipo == "sancao":
            tipo_pessoa = str(corpo.get("tipo_pessoa") or "").strip().lower()
            alvo = str(corpo.get("alvo") or corpo.get("numero") or corpo.get("nome") or "").strip()
            sancao = str(corpo.get("sancao") or corpo.get("tipo_sancao") or "").strip().lower()

            if not tipo_pessoa or not alvo or not sancao:
                estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    origem="SINCRONIZAR_SANCAO_INVALIDA"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": "Sanção incompleta ignorada.",
                    **estado_atual
                }, 200)

            ok, retorno = registrar_sancao_partida(
                partida_id,
                competicao,
                equipe,
                tipo_pessoa,
                alvo,
                sancao
            )

        elif tipo == "cartao_verde":
            tipo_pessoa = str(corpo.get("tipo_pessoa") or "").strip().lower()
            alvo = str(corpo.get("alvo") or corpo.get("numero") or corpo.get("nome") or "").strip()

            if not tipo_pessoa or not alvo:
                estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
                estado_atual = _emitir_estado_e_placar(
                    partida_id,
                    competicao,
                    estado_atual,
                    origem="SINCRONIZAR_VERDE_INVALIDO"
                )
                return _json_no_cache({
                    "ok": True,
                    "ignorado": True,
                    "mensagem": "Cartão verde incompleto ignorado.",
                    **estado_atual
                }, 200)

            ok, retorno = registrar_cartao_verde_partida(
                partida_id,
                competicao,
                equipe,
                tipo_pessoa,
                alvo
            )

        else:
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            estado_atual = _emitir_estado_e_placar(
                partida_id,
                competicao,
                estado_atual,
                origem="SINCRONIZAR_INVALIDO"
            )
            return _json_no_cache({
                "ok": True,
                "ignorado": True,
                "mensagem": f"Ação inválida ignorada na sincronização: {tipo or '-'}",
                **estado_atual
            }, 200)

        if not ok:
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            estado_atual = _emitir_estado_e_placar(
                partida_id,
                competicao,
                estado_atual,
                origem=f"SINCRONIZAR_{tipo.upper()}_FALHOU"
            )
            return _json_no_cache({
                "ok": True,
                "ignorado": True,
                "mensagem": retorno if isinstance(retorno, str) else "Ação ignorada.",
                **estado_atual
            }, 200)

        estado = _normalizar_estado_pos_acao(
            partida_id,
            competicao,
            retorno,
            origem=f"SINCRONIZAR_{tipo.upper()}"
        )

        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        estado_atual = obter_estado_cache(partida_id) or {}
        return _json_no_cache({
            "ok": True,
            "ignorado": True,
            "mensagem": f"Sincronização ignorada: {e}",
            **estado_atual
        }, 200)
    

@apontadores_bp.route("/apontador/estado/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def estado_jogo_view(competicao, partida_id):
    try:
        partida = buscar_partida_operacional(partida_id, competicao)

        if not partida:
            return _json_no_cache({"ok": False, "mensagem": "Partida não encontrada"}, 404)

        # Primeiro tenta o cache vivo. Evita consultar várias tabelas a cada sync/fallback.
        estado_cache = obter_estado_cache(partida_id) or {}
        veio_do_cache = bool(estado_cache)
        estado = dict(estado_cache)
        if not estado:
            garantir_estado_partida(partida_id, competicao)
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

        # MODO LEVE: usado por tabela/minhas partidas/visualizações pequenas.
        # Não busca papeleta, não reconcilia eventos, não calcula escudos/cores e não monta rotação.
        # Isso derruba drasticamente a carga de polling sem mudar o painel completo do jogo.
        if str(request.args.get("leve") or "").strip() == "1":
            pontos_a = int(estado.get("pontos_a") or estado.get("placar_a") or 0)
            pontos_b = int(estado.get("pontos_b") or estado.get("placar_b") or 0)
            sets_a = int(estado.get("sets_a") or partida.get("sets_a") or 0)
            sets_b = int(estado.get("sets_b") or partida.get("sets_b") or 0)
            fase_estado = str(estado.get("fase_partida") or estado.get("status_jogo") or "").lower().strip()
            fase_partida = str(partida.get("fase_partida") or partida.get("status_jogo") or partida.get("status") or "").lower().strip()
            finalizada = fase_estado in ("encerrado", "finalizada", "finalizado") or fase_partida in ("encerrado", "finalizada", "finalizado")
            return _json_no_cache({
                "ok": True,
                "leve": True,
                "pontos_a": pontos_a,
                "pontos_b": pontos_b,
                "placar_a": pontos_a,
                "placar_b": pontos_b,
                "placar_exibicao_a": pontos_a,
                "placar_exibicao_b": pontos_b,
                "sets_a": sets_a,
                "sets_b": sets_b,
                "set_atual": int(estado.get("set_atual") or partida.get("set_atual") or 1),
                "partida_finalizada": finalizada
            })

        equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
            partida_id, competicao, partida, estado
        )

        historico = estado.get("historico") or []
        ultima_acao = estado.get("ultima_acao") or (historico[0].get("descricao") if historico and isinstance(historico[0], dict) else "-")

        pontos_a = int(estado.get("pontos_a") or estado.get("placar_a") or 0)
        pontos_b = int(estado.get("pontos_b") or estado.get("placar_b") or 0)

        rotacao_a = list(estado.get("rotacao_a") or [])
        rotacao_b = list(estado.get("rotacao_b") or [])

        if not any(str(x).strip() for x in rotacao_a):
            rotacao_a = _rotacao_fallback_por_papeleta(papeleta_a)

        if not any(str(x).strip() for x in rotacao_b):
            rotacao_b = _rotacao_fallback_por_papeleta(papeleta_b)

        estado["rotacao_a"] = rotacao_a
        estado["rotacao_b"] = rotacao_b

        tempos_a = estado.get("tempos_a")
        tempos_b = estado.get("tempos_b")

        estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)
        # Se já existe cache vivo, não consulta eventos no banco em todo sync do apontador.
        # Reconciliar fica reservado para abertura sem cache ou chamada manual ?reconciliar=1.
        if (not veio_do_cache) or str(request.args.get("reconciliar") or "").strip() == "1":
            estado = _reconciliar_placar_com_eventos(partida_id, competicao, estado)
        equipe_a_op = partida.get("equipe_a_operacional") or partida.get("equipe_a") or estado.get("equipe_a") or ""
        equipe_b_op = partida.get("equipe_b_operacional") or partida.get("equipe_b") or estado.get("equipe_b") or ""
        estado = _aplicar_escudos_estado(estado, competicao, equipe_a_op, equipe_b_op)
        pontos_a = int(estado.get("pontos_a") or estado.get("placar_a") or pontos_a or 0)
        pontos_b = int(estado.get("pontos_b") or estado.get("placar_b") or pontos_b or 0)
        tempos_a = estado.get("tempos_a", tempos_a)
        tempos_b = estado.get("tempos_b", tempos_b)

        return _json_no_cache({
            "ok": True,
            "pontos_a": pontos_a,
            "pontos_b": pontos_b,
            "placar_a": pontos_a,
            "placar_b": pontos_b,
            "sets_a": int(estado.get("sets_a") or 0),
            "sets_b": int(estado.get("sets_b") or 0),
            "set_atual": int(estado.get("set_atual") or 1),
            "saque_atual": estado.get("saque_atual") or "",
            "equipe_a": equipe_a_op,
            "equipe_b": equipe_b_op,
            "equipe_a_operacional": equipe_a_op,
            "equipe_b_operacional": equipe_b_op,
            "escudo_a": _escudo_payload_leve(estado.get("escudo_a")),
            "escudo_b": _escudo_payload_leve(estado.get("escudo_b")),
            "escudo_a_operacional": _escudo_payload_leve(estado.get("escudo_a_operacional") or estado.get("escudo_a")),
            "escudo_b_operacional": _escudo_payload_leve(estado.get("escudo_b_operacional") or estado.get("escudo_b")),
            "cor_a": estado.get("cor_a") or "#2E6BE6",
            "cor_b": estado.get("cor_b") or "#E53935",
            "tempos_a": tempos_a,
            "tempos_b": tempos_b,
            "subs_a": int(estado.get("subs_a") or 0),
            "subs_b": int(estado.get("subs_b") or 0),
            "limite_tempos": int(estado.get("limite_tempos") or 2),
            "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
            "pontos_set": int(estado.get("pontos_set") or 25),
            "pontos_tiebreak": int(estado.get("pontos_tiebreak") or 15),
            "diferenca_minima": int(estado.get("diferenca_minima") or 2),
            "sets_para_vencer": int(estado.get("sets_para_vencer") or 2),
            "sets_tipo": estado.get("sets_tipo") or "melhor_de_3",
            "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
            "rotacao_a": rotacao_a,
            "rotacao_b": rotacao_b,
            "rotacao": {
                "equipe_a": rotacao_a,
                "equipe_b": rotacao_b
            },
            "status_jogadores_a": estado.get("status_jogadores_a") or {},
            "status_jogadores_b": estado.get("status_jogadores_b") or {},
            "sancoes_a": _lista_curta(estado.get("sancoes_a"), 8),
            "sancoes_b": _lista_curta(estado.get("sancoes_b"), 8),
            "cartoes_verdes_a": _lista_curta(estado.get("cartoes_verdes_a"), 8),
            "cartoes_verdes_b": _lista_curta(estado.get("cartoes_verdes_b"), 8),
            "historico": _lista_curta(historico, 5),
            "ultima_acao": ultima_acao,
            "partida_finalizada": str(estado.get("fase_partida") or "").lower() == "encerrado"
        })

    except Exception as e:
        print("ERRO estado_jogo_view:", e)
        return _json_no_cache({
            "ok": False,
            "mensagem": "Erro interno ao carregar estado do jogo."
        }, 500)




def _persistir_eventos_finais_partida(partida_id, competicao, eventos):
    """
    Offline-first oficial: durante o jogo o navegador só guarda eventos.
    Esta função roda somente no encerramento e grava a súmula no banco em ordem.
    """
    if not isinstance(eventos, list):
        return []

    processados = []

    def _payload_evento(item):
        if not isinstance(item, dict):
            return "", {}
        tipo = str(item.get("tipo") or "").strip().lower()
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}

        # Compatibilidade com o modo offline-first:
        # pontos vêm como {"equipe": "A", "scout": {...}}.
        # Para salvar no banco ao finalizar a partida, achatamos o scout aqui.
        if tipo == "ponto" and isinstance(payload.get("scout"), dict):
            scout = payload.get("scout") or {}
            payload = {**payload, **scout}
            payload.setdefault("equipe_scout", scout.get("equipe_scout") or scout.get("responsavel_lado"))
            payload.setdefault("responsavel_lado", scout.get("responsavel_lado") or scout.get("equipe_scout"))

        return tipo, payload

    for item in eventos:
        tipo, payload = _payload_evento(item)
        if not tipo:
            continue

        equipe = str(payload.get("equipe") or payload.get("equipe_pontuadora") or "").strip().upper()
        try:
            if tipo == "ponto":
                tipo_lance_payload = str(payload.get("tipo_lance") or payload.get("resultado") or "ponto").strip().lower()

                # No modo offline-first, para erro/falta o campo "equipe" pode vir como
                # a equipe que cometeu a ação. Por isso salvamos sempre separado:
                # equipe_pontuadora = quem ganhou o ponto;
                # equipe_scout/responsavel_lado = quem cometeu erro/falta.
                if tipo_lance_payload in {"erro", "falta"}:
                    equipe_scout = str(payload.get("equipe_scout") or payload.get("responsavel_lado") or equipe).strip().upper()
                    if equipe_scout not in {"A", "B"}:
                        equipe_scout = equipe
                    equipe_pontuadora = str(payload.get("equipe_pontuadora") or "").strip().upper()
                    if equipe_pontuadora not in {"A", "B"}:
                        equipe_pontuadora = "B" if equipe_scout == "A" else "A"
                else:
                    equipe_pontuadora = str(payload.get("equipe_pontuadora") or equipe).strip().upper()
                    if equipe_pontuadora not in {"A", "B"}:
                        equipe_pontuadora = equipe
                    equipe_scout = equipe_pontuadora

                detalhes = {
                    "fundamento": payload.get("fundamento") or payload.get("detalhe_lance") or "",
                    "resultado": payload.get("resultado") or payload.get("tipo_lance") or "ponto",
                    "tipo_lance": payload.get("tipo_lance") or payload.get("resultado") or "ponto",
                    "detalhe_lance": payload.get("detalhe_lance") or payload.get("fundamento") or "",
                    "tipo_erro": payload.get("tipo_erro") or "",
                    "atleta_numero": payload.get("atleta_numero") or "",
                    "atleta_nome": payload.get("atleta_nome") or "",
                    "atleta_label": payload.get("atleta_label") or "",
                    "equipe_pontuadora": equipe_pontuadora,
                    "equipe_scout": equipe_scout,
                    "responsavel_lado": equipe_scout,
                }
                ok, retorno = registrar_ponto_partida(partida_id, competicao, equipe_pontuadora, "ponto", detalhes)

            elif tipo == "tempo":
                ok, retorno = registrar_tempo_partida(partida_id, competicao, equipe)

            elif tipo == "substituicao":
                ok, retorno = registrar_substituicao_partida(
                    partida_id, competicao, equipe,
                    str(payload.get("numero_sai") or "").strip(),
                    str(payload.get("numero_entra") or "").strip()
                )

            elif tipo == "substituicao_excepcional":
                try:
                    ok, retorno = registrar_substituicao_excepcional_partida(
                        partida_id, competicao, equipe,
                        str(payload.get("numero_sai") or "").strip(),
                        str(payload.get("numero_entra") or "").strip(),
                        str(payload.get("motivo") or "").strip()
                    )
                except TypeError:
                    ok, retorno = registrar_substituicao_excepcional_partida(
                        partida_id, competicao, equipe,
                        str(payload.get("numero_sai") or "").strip(),
                        str(payload.get("numero_entra") or "").strip()
                    )

            elif tipo == "retardamento":
                ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)

            elif tipo == "sancao":
                ok, retorno = registrar_sancao_partida(
                    partida_id, competicao, equipe,
                    str(payload.get("tipo_pessoa") or "").strip().lower(),
                    str(payload.get("alvo") or payload.get("numero") or payload.get("nome") or "").strip(),
                    str(payload.get("sancao") or payload.get("tipo_sancao") or "").strip().lower()
                )

            elif tipo == "cartao_verde":
                ok, retorno = registrar_cartao_verde_partida(
                    partida_id, competicao, equipe,
                    str(payload.get("tipo_pessoa") or "").strip().lower(),
                    str(payload.get("alvo") or payload.get("numero") or payload.get("nome") or "").strip()
                )

            else:
                processados.append({"tipo": tipo, "ok": True, "ignorado": True})
                continue

            processados.append({"tipo": tipo, "ok": bool(ok)})
            if not ok:
                print(f"AVISO salvar final ignorou {tipo}: {retorno}", flush=True)

        except Exception as e:
            processados.append({"tipo": tipo, "ok": False, "erro": str(e)})
            print(f"ERRO salvar evento final {tipo}:", e, flush=True)

    return processados

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/encerrar", methods=["POST"])
@exigir_perfil("apontador")
def encerrar_partida_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        observacoes = ""
        if request.is_json:
            observacoes = (corpo.get("observacoes") or "").strip()
        else:
            observacoes = (request.form.get("observacoes") or "").strip()

        eventos = corpo.get("eventos") if isinstance(corpo, dict) else []
        estado_final_cliente = corpo.get("estado_final") if isinstance(corpo.get("estado_final"), dict) else {}

        # ÚNICO MOMENTO EM QUE A FILA LOCAL DO APONTADOR VAI PARA O BANCO.
        processados = _persistir_eventos_finais_partida(partida_id, competicao, eventos)

        estado = buscar_estado_jogo_partida(partida_id, competicao)
        if not estado:
            estado = dict(obter_estado_cache(partida_id) or estado_final_cliente or {})

        encerrar_partida(partida_id, competicao, observacoes)
        resultado_avanco = _atualizar_avanco_apos_finalizacao(competicao)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or estado or {}
        if resultado_avanco:
            estado["avanco_atualizado"] = resultado_avanco
        estado["encerrado"] = True
        estado["partida_finalizada"] = True
        estado["status_jogo"] = "finalizada"
        estado["eventos_processados_final"] = processados

        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="ENCERRAR_PARTIDA_FINAL_OFFLINE")

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida salva no banco e encerrada com sucesso.",
            "encerrado": True,
            "estado": estado,
            "partida_finalizada": True,
            "abrir_observacoes": True,
            "url_observacoes": url_for("apontadores.observacoes_view", competicao=competicao, partida_id=partida_id),
            "eventos_processados": processados,
            **estado
        })
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao encerrar partida: {e}"}, 500)


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def observacoes_view(competicao, partida_id):
    dados_finalizacao = listar_dados_finalizacao_partida(partida_id, competicao) or {}
    partida = dados_finalizacao.get("partida") or buscar_partida_operacional(partida_id, competicao)

    return render_template(
        "observacoes.html",
        partida=partida,
        competicao_nome=competicao,
        dados_finalizacao=dados_finalizacao,
        equipes_finalizacao=dados_finalizacao.get("equipes", []),
        destaques_partida=dados_finalizacao.get("destaques_partida", []),
    )


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_observacoes_view(competicao, partida_id):
    observacoes = request.form.get("observacoes")

    destaque_lado = (request.form.get("destaque_lado") or "").strip().upper()
    destaque_atleta_id = (request.form.get("destaque_atleta_id") or "").strip()
    destaque_numero = (request.form.get("destaque_numero") or "").strip()
    destaque_nome = (request.form.get("destaque_nome") or "").strip()
    destaque_observacao = (request.form.get("destaque_observacao") or "").strip()

    if destaque_lado and (destaque_atleta_id or destaque_numero or destaque_nome):
        ok_destaque, msg_destaque = salvar_destaque_partida(
            partida_id,
            competicao,
            destaque_lado,
            atleta_id=destaque_atleta_id,
            numero=destaque_numero,
            nome=destaque_nome,
            observacao=destaque_observacao,
        )
        if not ok_destaque:
            flash(msg_destaque or "Não foi possível salvar o destaque.", "erro")
            return redirect(url_for("apontadores.observacoes_view", competicao=competicao, partida_id=partida_id))
        flash(msg_destaque or "Destaque salvo com sucesso.", "sucesso")

    encerrar_partida(partida_id, competicao, observacoes)

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
    estado["encerrado"] = True
    estado["partida_finalizada"] = True
    estado["status_jogo"] = "finalizada"
    _emitir_estado_e_placar(partida_id, competicao, estado, origem="SALVAR_FINALIZACAO")

    return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

# FIX: garantir fundamento/resultado corretos para falta e erro_saque

@apontadores_bp.route("/apontador/inverter-lados/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def inverter_lados(partida_id):
    competicao = session.get("competicao_apontador") or ""
    estado = obter_estado_cache(partida_id) or {}

    if competicao and not estado:
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    estado["invertido"] = not bool(estado.get("invertido", False))

    if competicao:
        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="INVERTER_LADOS")
    else:
        apontador_login = _login_apontador_sessao() or estado.get("apontador") or ""
        if apontador_login:
            estado["apontador"] = apontador_login
        emitir_estado_partida(partida_id, estado)
        emitir_placar_apontador(apontador_login, partida_id, estado)

    return _json_no_cache({
        "ok": True,
        "invertido": estado["invertido"]
    })



@apontadores_bp.route("/telao", methods=["GET", "POST"])
def telao_por_pin():
    """
    Entrada pública do telão por PIN operacional.
    O telão exibido continua sendo o placar profissional já existente.
    """
    if request.method == "POST":
        pin = (request.form.get("pin") or "").strip()
        pin_limpo = "".join(ch for ch in pin if ch.isdigit())

        if len(pin_limpo) != 4:
            flash("Digite um PIN de 4 números.", "erro")
            return redirect(url_for("apontadores.telao_por_pin"))

        vinculo = buscar_vinculo_operacional_por_pin(pin_limpo)
        if not vinculo:
            flash("PIN não encontrado ou inativo.", "erro")
            return redirect(url_for("apontadores.telao_por_pin"))

        session["telao_pin_validado"] = True
        session["telao_pin"] = pin_limpo
        session["telao_competicao"] = vinculo.get("competicao") or ""
        session["telao_apontador"] = vinculo.get("apontador_cpf") or ""
        session["telao_apontador_nome"] = vinculo.get("apontador_nome") or ""
        return redirect(url_for("apontadores.telao_por_pin"))

    if request.args.get("trocar") == "1":
        for chave in ["telao_pin_validado", "telao_pin", "telao_competicao", "telao_apontador", "telao_apontador_nome"]:
            session.pop(chave, None)
        return redirect(url_for("apontadores.telao_por_pin"))

    if session.get("telao_pin_validado") and session.get("telao_apontador"):
        from socket_events import obter_ultimo_placar_apontador
        apontador = session.get("telao_apontador") or ""
        estado = obter_ultimo_placar_apontador(apontador) or {}
        return render_template(
            "placar_profissional.html",
            estado=estado,
            partida=estado,
            apontador=apontador,
            pin=session.get("telao_pin") or "",
            competicao=session.get("telao_competicao") or "",
        )

    return render_template("pin_telao.html")


@apontadores_bp.route("/placar-ao-vivo")
def placar_ao_vivo_redirect():
    apontador = _login_apontador_sessao() or ""

    if apontador:
        return redirect(url_for("apontadores.placar_ao_vivo_apontador", apontador=apontador))

    return render_template(
        "placar_profissional.html",
        estado={},
        partida={},
        apontador=""
    )


@apontadores_bp.route("/placar-ao-vivo/<apontador>")
def placar_ao_vivo_apontador(apontador):
    from socket_events import obter_ultimo_placar_apontador

    estado = obter_ultimo_placar_apontador(apontador) or {}

    return render_template(
        "placar_profissional.html",
        estado=estado,
        partida=estado,
        apontador=apontador
    )
