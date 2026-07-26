from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify
from functools import wraps
import random
import json
import os
import time
import hashlib

from banco import (
    buscar_competicao_por_organizador,
    buscar_competicao_por_nome,
    listar_equipes_da_competicao,
    criar_grupo,
    listar_grupos,
    adicionar_equipe_no_grupo,
    listar_equipes_por_grupo,
    listar_equipes_por_grupos_competicao,
    criar_partida,
    listar_partidas,
    buscar_partida_por_id,
    buscar_estado_jogo_partida,
    listar_eventos_partida,
    criar_tabela_destaques_partida,
    limpar_partidas,
    limpar_partidas_por_fase,
    remover_equipe_do_grupo,
    excluir_grupo as excluir_grupo_banco,
    excluir_partida as excluir_partida_banco,
    atualizar_partida,
    competicao_esta_travada,
    fase_grupos_esta_travada_por_jogo,
    fase_partidas_pode_ser_alterada,
    fase_tem_partida_iniciada,
    listar_quadras_competicao,
    garantir_quadras_competicao,
    buscar_quadra_competicao_por_id,
    buscar_quadra_competicao_por_texto,
    formatar_quadra_exibicao,
    normalizar_vinculos_quadras_competicao,
    vincular_grupo_a_quadra,
    aplicar_quadra_em_partida,
    conectar,
    buscar_configuracao_agenda_competicao,
    atualizar_configuracao_agenda_competicao,
    inicializar_configuracao_agenda_competicao,
    _buscar_colunas_tabela,
    buscar_avanco_config_competicao,
    gerar_partidas_avanco_competicao,
    status_avanco_classificatorias_competicao,
    avanco_ja_gerado_competicao,
    limpar_partidas_avanco_nao_iniciadas_competicao,
    assinatura_classificacao_competicao,
    obter_cache_classificacao,
    salvar_cache_classificacao,
    buscar_data_hora_rodada_programada,
    garantir_codigo_publico_competicao,
    buscar_competicao_por_codigo_publico,
)

from routes.utils import exigir_perfil, aplicar_placar_exibicao_partida
from socket_events import obter_estado_cache, obter_estado_versao

tabela_bp = Blueprint("tabela", __name__)


# =========================================================
# CACHE LEVE DA TABELA / VISUALIZADOR
# =========================================================
# Evita que cada troca de aba ou atualização do navegador faça as mesmas
# consultas pesadas de grupos, equipes, partidas, quadras e avanço.
# O cache é curto e é limpo automaticamente em qualquer POST da tabela.
_TABELA_CACHE = {}
_TABELA_CACHE_TTL = int(os.environ.get("TABELA_CACHE_TTL", "20") or 20)
_TABELA_CACHE_MAX_ITENS = int(os.environ.get("TABELA_CACHE_MAX_ITENS", "120") or 120)


def _cache_agora():
    try:
        return time.time()
    except Exception:
        return 0


def _cache_key(*partes):
    return tuple(str(p or "").strip() for p in partes)


def _cache_get_tabela(chave, ttl=None):
    ttl = _TABELA_CACHE_TTL if ttl is None else ttl
    item = _TABELA_CACHE.get(chave)
    if not item:
        return None
    criado, valor = item
    if (_cache_agora() - criado) > ttl:
        _TABELA_CACHE.pop(chave, None)
        return None
    return valor


def _cache_set_tabela(chave, valor):
    if len(_TABELA_CACHE) > _TABELA_CACHE_MAX_ITENS:
        _TABELA_CACHE.clear()
    _TABELA_CACHE[chave] = (_cache_agora(), valor)
    return valor


def _limpar_cache_tabela(competicao_nome=None):
    if not competicao_nome:
        _TABELA_CACHE.clear()
        return
    nome = str(competicao_nome or "").strip()
    for chave in list(_TABELA_CACHE.keys()):
        if nome in chave:
            _TABELA_CACHE.pop(chave, None)


def _limpar_cache_tabela_e_classificacao(competicao_nome=None):
    """Limpa o cache curto da tabela e deixa a próxima leitura recalcular.

    Mantém os dados do banco intactos. É chamada somente depois de alterações
    reais (gerar, editar, excluir, salvar grupo/partida).
    """
    _limpar_cache_tabela(competicao_nome)


def _assinatura_classificacao_local(competicao_nome, partidas_preparadas, grupos, competicao):
    """Assinatura em memória, sem consulta extra ao banco.

    A versão anterior chamava assinatura_classificacao_competicao(), que fazia
    hash no PostgreSQL varrendo partidas/grupos. Isso tornava cada abertura da
    tabela e cada geração de fase dependente de mais uma consulta pesada.
    Aqui usamos os dados que a rota já carregou.
    """
    base = {
        "competicao": competicao_nome,
        "criterios": (competicao or {}).get("criterios_desempate") or (competicao or {}).get("criterios_classificacao") or "",
        "sets_tipo": (competicao or {}).get("sets_tipo") or "",
        "grupos": [
            {
                "grupo": (g.get("grupo") or {}).get("nome"),
                "equipes": sorted(str(e.get("equipe") or "") for e in (g.get("equipes") or [])),
            }
            for g in (grupos or [])
        ],
        "partidas": [
            {
                "id": p.get("id"),
                "grupo": p.get("grupo"),
                "fase": p.get("fase_normalizada") or p.get("fase"),
                "a": p.get("equipe_a"),
                "b": p.get("equipe_b"),
                "status": p.get("status_normalizado") or p.get("status"),
                "sets_a": p.get("sets_a"),
                "sets_b": p.get("sets_b"),
                "pontos_a": p.get("pontos_a"),
                "pontos_b": p.get("pontos_b"),
                "set1_a": p.get("set1_a"), "set1_b": p.get("set1_b"),
                "set2_a": p.get("set2_a"), "set2_b": p.get("set2_b"),
                "set3_a": p.get("set3_a"), "set3_b": p.get("set3_b"),
                "set4_a": p.get("set4_a"), "set4_b": p.get("set4_b"),
                "set5_a": p.get("set5_a"), "set5_b": p.get("set5_b"),
            }
            for p in (partidas_preparadas or [])
            if _partida_esta_finalizada(p)
        ],
    }
    texto = json.dumps(base, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(texto.encode("utf-8")).hexdigest()


def _partidas_finalizadas_por_grupo(partidas):
    mapa = {}
    for p in partidas or []:
        if not _partida_esta_finalizada(p):
            continue
        grupo = p.get("grupo")
        if not grupo:
            continue
        mapa.setdefault(grupo, []).append(p)
    return mapa


def _partida_tem_resultado_ou_iniciou(partida):
    """Proteção para nunca apagar jogo com placar/resultado/status real."""
    if not partida:
        return False
    if _partida_conta_como_iniciada_para_trava(partida) or _partida_esta_finalizada(partida):
        return True
    for campo in ("sets_a", "sets_b", "pontos_a", "pontos_b", "placar_a", "placar_b", "set1_a", "set1_b", "set2_a", "set2_b", "set3_a", "set3_b", "set4_a", "set4_b", "set5_a", "set5_b"):
        try:
            if int((partida or {}).get(campo) or 0) > 0:
                return True
        except Exception:
            if str((partida or {}).get(campo) or "").strip() not in {"", "0", "None", "null"}:
                return True
    return False


def _limpar_partidas_fase_serie_nao_iniciadas(nome_competicao, fase_banco, serie=""):
    """Remove somente jogos automáticos NÃO iniciados da fase/série atual.

    Não apaga finalizadas, ao vivo, pré-jogo iniciado, nem partidas com sets/pontos.
    Também evita que gerar Prata mexa na Ouro, porque filtra pelo prefixo
    origem='avanco:<serie>:' quando houver série.
    """
    fase_banco = _fase_subaba_para_banco(fase_banco)
    serie = str(serie or "").strip().lower()
    partidas = _listar_partidas_cache(nome_competicao) or []
    ids = []
    for p in partidas:
        if _fase_partida_normalizada(p) != fase_banco:
            continue
        if serie:
            serie_p, _jogo_id = _origem_partida_avanco(p)
            if serie_p and serie_p != serie:
                continue
            if not serie_p and str(p.get("origem") or "").startswith("avanco:"):
                continue
        if _partida_tem_resultado_ou_iniciou(p):
            continue
        ids.append(p.get("id"))

    ids = [i for i in ids if i]
    if not ids:
        return 0

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM partidas WHERE competicao = %s AND id = ANY(%s)", (nome_competicao, ids))
        conn.commit()
    return len(ids)


def _vencedor_partida_rapido(partida, placeholder="Vencedor"):
    """Resolve vencedor com várias compatibilidades de coluna."""
    if not partida:
        return placeholder
    for campo in ("vencedor", "equipe_vencedora", "ganhador"):
        valor = str(partida.get(campo) or "").strip()
        if valor and valor.lower() not in {"a definir", "none", "null"}:
            return valor
    if _partida_esta_finalizada(partida):
        try:
            sets_a = int(partida.get("sets_a") or 0)
            sets_b = int(partida.get("sets_b") or 0)
            if sets_a > sets_b:
                return partida.get("equipe_a") or placeholder
            if sets_b > sets_a:
                return partida.get("equipe_b") or placeholder
        except Exception:
            pass
    return placeholder


def _ordenar_classificados_intercalado(classificacao):
    """Retorna equipes intercalando posições dos grupos: 1ºA, 1ºB, 2ºA..."""
    classificados = []
    maior = max((len(linhas) for linhas in (classificacao or {}).values()), default=0)
    for posicao in range(maior):
        for nome_grupo in sorted((classificacao or {}).keys()):
            linhas = classificacao.get(nome_grupo) or []
            if posicao < len(linhas):
                equipe = linhas[posicao].get("equipe")
                if equipe:
                    classificados.append(equipe)
    return classificados



@tabela_bp.after_request
def _invalidar_cache_tabela_apos_post(response):
    """Qualquer alteração na tabela/grupos/partidas limpa o cache curto.

    Assim o GET seguinte já busca dados novos, mas os GETs repetidos de várias
    abas/celulares continuam rápidos e sem martelar o banco.
    """
    try:
        if request.method == "POST" and request.path.startswith("/tabela"):
            usuario = session.get("usuario")
            if usuario:
                comp = buscar_competicao_por_organizador(usuario) or {}
                _limpar_cache_tabela(comp.get("nome"))
            else:
                _limpar_cache_tabela()
    except Exception as e:
        print("AVISO tabela/cache after_request:", repr(e))
    return response


def _buscar_competicao_organizador_cache(usuario):
    chave = _cache_key("competicao_organizador", usuario)
    cached = _cache_get_tabela(chave, ttl=30)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, buscar_competicao_por_organizador(usuario))


def _listar_grupos_cache(competicao_nome):
    chave = _cache_key("grupos", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, listar_grupos(competicao_nome) or [])


def _listar_equipes_competicao_cache(competicao_nome):
    chave = _cache_key("equipes", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, listar_equipes_da_competicao(competicao_nome) or [])


def _listar_partidas_cache(competicao_nome):
    chave = _cache_key("partidas", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, listar_partidas(competicao_nome) or [])


def _quadras_cache(competicao_nome, qtd_quadras=1):
    chave = _cache_key("quadras", competicao_nome, qtd_quadras)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, garantir_quadras_competicao(competicao_nome, qtd_quadras or 1) or [])


def _config_agenda_cache(competicao_nome):
    chave = _cache_key("config_agenda", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    try:
        inicializar_configuracao_agenda_competicao(competicao_nome)
    except Exception as e:
        print("AVISO tabela/inicializar_config_agenda:", repr(e))
    return _cache_set_tabela(chave, buscar_configuracao_agenda_competicao(competicao_nome))


def _avanco_cache(competicao_nome):
    chave = _cache_key("avanco", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    return _cache_set_tabela(chave, buscar_avanco_config_competicao(competicao_nome) or {})


def _status_avanco_cache(competicao_nome):
    chave = _cache_key("status_avanco", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return dict(cached)
    status = status_avanco_classificatorias_competicao(competicao_nome) or {}
    return dict(_cache_set_tabela(chave, status))


def _avanco_gerado_cache(competicao_nome):
    chave = _cache_key("avanco_gerado", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return bool(cached)
    return bool(_cache_set_tabela(chave, avanco_ja_gerado_competicao(competicao_nome)))


def _competicao_travada_cache(competicao_nome):
    chave = _cache_key("competicao_travada", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return bool(cached)
    return bool(_cache_set_tabela(chave, competicao_esta_travada(competicao_nome)))


def _grupos_travados_cache(competicao_nome):
    chave = _cache_key("grupos_travados", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return bool(cached)
    return bool(_cache_set_tabela(chave, fase_grupos_esta_travada_por_jogo(competicao_nome)))


def _fase_atual_travada_cache(competicao_nome, fase_banco):
    """Cacheia a trava por fase.

    Antes cada abertura da tabela varria partidas para descobrir se a fase
    já tinha jogo iniciado. Isso era repetido mesmo quando nada mudava.
    """
    chave = _cache_key("fase_travada", competicao_nome, fase_banco)
    cached = _cache_get_tabela(chave, ttl=10)
    if cached is not None:
        return bool(cached)
    travada = not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao_nome, fase_banco)
    return bool(_cache_set_tabela(chave, travada))


def _pacote_cache_get(nome_competicao, aba, fase_subaba, serie_ativa=""):
    chave = _cache_key("pacote_tabela", nome_competicao, aba, fase_subaba, serie_ativa)
    return _cache_get_tabela(chave, ttl=_TABELA_CACHE_TTL)


def _pacote_cache_set(nome_competicao, aba, fase_subaba, serie_ativa, pacote):
    chave = _cache_key("pacote_tabela", nome_competicao, aba, fase_subaba, serie_ativa)
    return _cache_set_tabela(chave, pacote)


# =========================================================
# PERMISSÃO ROBUSTA DA TABELA
# =========================================================
def exigir_organizador_da_competicao(func):
    """
    Evita falso bloqueio de perfil.
    Algumas sessões antigas podem ter perfil escrito de forma diferente,
    mas se o usuário logado possui competição vinculada como organizador,
    ele pode acessar e alterar a tabela.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        usuario = session.get("usuario")

        if not usuario:
            flash("Sessão expirada. Faça login novamente.", "erro")
            return redirect(url_for("auth.login"))

        perfil = (session.get("perfil") or "").strip().lower()
        if perfil in {"organizador", "superadmin"}:
            return func(*args, **kwargs)

        competicao = _buscar_competicao_organizador_cache(usuario)
        if competicao:
            return func(*args, **kwargs)

        flash("Você não tem permissão para acessar esta área.", "erro")
        return redirect(url_for("painel.inicio"))

    return wrapper




def _remover_flash_permissao_falso():
    """Remove aviso antigo de permissão quando a tela da tabela foi carregada com acesso válido.

    Esse flash pode ficar pendurado na sessão quando alguma rota anterior gerou
    o aviso, mas a tela atual foi liberada corretamente pelo organizador.
    """
    flashes = session.get("_flashes") or []
    if not flashes:
        return

    session["_flashes"] = [
        item for item in flashes
        if not (
            isinstance(item, (list, tuple))
            and len(item) >= 2
            and str(item[1]).strip() == "Você não tem permissão para acessar esta área."
        )
    ]


def _grupos_com_equipes_cacheados(competicao_nome, grupos_raw=None, incluir_quadra=True):
    """Monta grupos com suas equipes usando 1 consulta para todas as equipes dos grupos.

    Evita o padrão lento: for grupo -> listar_equipes_por_grupo(grupo_id).
    Mantém fallback individual caso a função nova falhe em algum banco antigo.
    """
    grupos_raw = grupos_raw if grupos_raw is not None else (listar_grupos(competicao_nome) or [])
    try:
        equipes_por_grupo = listar_equipes_por_grupos_competicao(competicao_nome) or {}
    except Exception as e:
        print("AVISO grupos_cacheados/listar_equipes_por_grupos_competicao:", repr(e))
        equipes_por_grupo = None

    grupos = []
    for g in grupos_raw or []:
        gid = g.get("id")
        equipes_grupo = (equipes_por_grupo or {}).get(gid)
        if equipes_grupo is None:
            equipes_grupo = listar_equipes_por_grupo(gid) or []

        item = {"grupo": g, "equipes": equipes_grupo}
        if incluir_quadra:
            quadra_id = _quadra_id_do_grupo(g)
            item.update({
                "quadra_label": _quadra_label_por_id(competicao_nome, quadra_id),
                "quadra_id": quadra_id,
            })
        grupos.append(item)
    return grupos


# =========================================================
# HELPERS
# =========================================================
def _fases_disponiveis(competicao):
    formato_finais = (competicao.get("formato_finais") or "").strip().lower()

    tem_quartas = formato_finais == "quartas"
    tem_semifinais = formato_finais in {"quartas", "semifinal"}
    tem_finais = True

    return {
        "tem_quartas": tem_quartas,
        "tem_semifinais": tem_semifinais,
        "tem_finais": tem_finais,
    }


def _fase_subaba_para_banco(fase_subaba):
    fase_subaba = (fase_subaba or "classificatorias").strip().lower()
    mapa = {
        "classificatorias": "grupos",
        "oitavas": "oitavas",
        "quartas": "quartas",
        "semifinais": "semifinal",
        "semifinal": "semifinal",
        "finais": "final",
        "final": "final",
        "terceiro_lugar": "terceiro_lugar",
        "terceiro": "terceiro_lugar",
    }
    return mapa.get(fase_subaba, "grupos")


def _fase_subaba_canonica(fase_subaba):
    fase_banco = _fase_subaba_para_banco(fase_subaba)
    mapa = {
        "grupos": "classificatorias",
        "semifinal": "semifinal",
        "final": "final",
    }
    return mapa.get(fase_banco, fase_banco)


def _nome_fase_mata_mata(fase_subaba):
    mapa = {
        "oitavas": "Oitavas",
        "quartas": "Quartas",
        "semifinais": "Semifinal",
        "semifinal": "Semifinal",
        "finais": "Final",
        "final": "Final",
        "terceiro_lugar": "3º lugar",
    }
    return mapa.get((fase_subaba or "").strip().lower(), "")


FASES_AVANCO_ORDEM = ["oitavas", "quartas", "semifinal", "terceiro_lugar", "final"]
FASES_AVANCO_LABELS = {
    "classificatorias": "Classificatórias",
    "oitavas": "Oitavas",
    "quartas": "Quartas",
    "semifinal": "Semifinal",
    "terceiro_lugar": "3º lugar",
    "final": "Final",
}


def _fases_do_avanco_para_tabela(avanco):
    fases = []
    for serie in (avanco or {}).get("series") or []:
        if not serie.get("ativa", True):
            continue
        for fase in serie.get("fases") or []:
            fase = _fase_subaba_canonica(fase)
            if fase != "classificatorias" and fase not in fases:
                fases.append(fase)

    for jogo in (avanco or {}).get("jogos") or []:
        fase = _fase_subaba_canonica(jogo.get("fase"))
        if fase != "classificatorias" and fase not in fases:
            fases.append(fase)

    return sorted(fases, key=lambda f: FASES_AVANCO_ORDEM.index(f) if f in FASES_AVANCO_ORDEM else 99)


def _series_do_avanco_por_fase(avanco, fase):
    fase = _fase_subaba_canonica(fase)
    series = []
    ids_com_jogo = {
        str(j.get("serie") or "").strip().lower()
        for j in ((avanco or {}).get("jogos") or [])
        if _fase_subaba_canonica(j.get("fase")) == fase
    }
    for serie in (avanco or {}).get("series") or []:
        sid = str(serie.get("id") or "").strip().lower()
        if not sid or not serie.get("ativa", True):
            continue
        fases = [_fase_subaba_canonica(f) for f in (serie.get("fases") or [])]
        if fase in fases or sid in ids_com_jogo:
            series.append({"id": sid, "nome": serie.get("nome") or sid.title()})
    return series


def _origem_partida_avanco(partida):
    origem = str((partida or {}).get("origem") or "").strip()
    if not origem.startswith("avanco:"):
        return "", ""
    partes = origem.split(":", 2)
    if len(partes) >= 3:
        return partes[1].strip().lower(), partes[2].strip()
    return "", ""


def _partida_eh_avanco(partida):
    return str((partida or {}).get("origem") or "").strip().startswith("avanco:")


def _filtrar_partidas_por_serie_avanco(partidas, serie):
    serie = str(serie or "").strip().lower()
    if not serie:
        return partidas
    filtradas = []
    for p in partidas or []:
        serie_p, _jogo_id = _origem_partida_avanco(p)
        if serie_p == serie or not serie_p:
            filtradas.append(p)
    return filtradas


def _label_origem_avanco(origem):
    origem = origem if isinstance(origem, dict) else {}
    return origem.get("label") or origem.get("valor") or "A definir"


def _montar_espelho_avanco(avanco, partidas, classificatorias_fechadas=False):
    """Monta o espelho público do avanço com placar e tempo real.

    Antes o visualizador recebia apenas equipes/status/proximos jogos. Por isso
    os cards do chaveamento (quartas, semifinal, 3º lugar e final) não tinham
    placar, sets, parciais nem dados suficientes para entrar no Socket.IO.
    Esta versão leva para o template os mesmos campos preparados para os cards
    normais de jogos.
    """
    def _int_seguro(valor, padrao=0):
        try:
            if valor in (None, ""):
                return padrao
            return int(valor)
        except (TypeError, ValueError):
            return padrao

    def _set_unico_partida(partida):
        if not partida:
            return False
        valor = partida.get("set_unico")
        if isinstance(valor, bool):
            return valor
        if str(valor or "").strip().lower() in {"1", "true", "sim", "yes", "on"}:
            return True
        tipo = str(
            partida.get("sets_tipo")
            or partida.get("tipo_sets")
            or partida.get("formato_sets")
            or ""
        ).strip().lower().replace("-", "_").replace(" ", "_")
        return tipo in {"set_unico", "unico", "único"}

    mapa_partidas = {}
    for p in partidas or []:
        serie, jogo_id = _origem_partida_avanco(p)
        if serie and jogo_id:
            mapa_partidas[(serie, jogo_id)] = p

    saida = []
    for serie in (avanco or {}).get("series") or []:
        if not serie.get("ativa", True):
            continue
        sid = str(serie.get("id") or "").strip().lower()
        if not sid:
            continue

        item_serie = {
            "id": sid,
            "nome": serie.get("nome") or sid.title(),
            "fases": [],
        }

        for fase in _fases_do_avanco_para_tabela({"series": [serie], "jogos": (avanco or {}).get("jogos") or []}):
            jogos = []
            for jogo in (avanco or {}).get("jogos") or []:
                if str(jogo.get("serie") or "").strip().lower() != sid:
                    continue
                if _fase_subaba_canonica(jogo.get("fase")) != fase:
                    continue

                jid = str(jogo.get("id") or "").strip()
                partida = mapa_partidas.get((sid, jid))

                equipe_a = (partida or {}).get("equipe_a") or ""
                equipe_b = (partida or {}).get("equipe_b") or ""
                if not classificatorias_fechadas:
                    equipe_a = ""
                    equipe_b = ""
                    partida = None

                finalizada = bool((partida or {}).get("finalizada")) or (bool(partida) and _partida_esta_finalizada(partida))
                ao_vivo = bool((partida or {}).get("ao_vivo")) or (bool(partida) and _partida_esta_ao_vivo(partida))
                set_unico = _set_unico_partida(partida)

                # No jogo AO VIVO o placar principal do card precisa ser o placar
                # do SET ATUAL (pontos_a/pontos_b). O placar_exibicao_a/b em
                # partidas melhor de 3/5 é o placar de SETS, por isso causava 0x0
                # no chaveamento mesmo com o apontador em 7x7.
                if ao_vivo and not finalizada:
                    placar_a = (partida or {}).get("pontos_a")
                    placar_b = (partida or {}).get("pontos_b")
                else:
                    placar_a = (
                        (partida or {}).get("placar_exibicao_a")
                        if (partida or {}).get("placar_exibicao_a") is not None
                        else (partida or {}).get("pontos_a")
                    )
                    placar_b = (
                        (partida or {}).get("placar_exibicao_b")
                        if (partida or {}).get("placar_exibicao_b") is not None
                        else (partida or {}).get("pontos_b")
                    )

                jogos.append({
                    "id": jid,
                    "ordem": jogo.get("ordem") or 999,
                    "fase": fase,
                    "origem_a_label": _label_origem_avanco(jogo.get("origem_a")),
                    "origem_b_label": _label_origem_avanco(jogo.get("origem_b")),
                    "equipe_a": "" if equipe_a in {"A definir", ""} else equipe_a,
                    "equipe_b": "" if equipe_b in {"A definir", ""} else equipe_b,
                    "partida_id": (partida or {}).get("id"),
                    "status_exibicao": (partida or {}).get("status_exibicao") or "Aguardando origem",
                    "ao_vivo": ao_vivo,
                    "finalizada": finalizada,
                    "set_unico": set_unico,
                    "set_atual": _int_seguro((partida or {}).get("set_atual"), 1),
                    "sets_a": _int_seguro((partida or {}).get("sets_a"), 0),
                    "sets_b": _int_seguro((partida or {}).get("sets_b"), 0),
                    "placar_exibicao_a": _int_seguro(placar_a, 0),
                    "placar_exibicao_b": _int_seguro(placar_b, 0),
                    "parciais_formatadas": (partida or {}).get("parciais_formatadas") or "-",
                    "escudo_a": (partida or {}).get("escudo_a") or (partida or {}).get("equipe_a_escudo") or "/static/img/escudo_padrao.svg",
                    "escudo_b": (partida or {}).get("escudo_b") or (partida or {}).get("equipe_b_escudo") or "/static/img/escudo_padrao.svg",
                    "quadra_label": (partida or {}).get("quadra_label") or (partida or {}).get("quadra_nome") or "Sem quadra",
                    "proximo_vencedor": jogo.get("proximo_vencedor") or "",
                    "proximo_perdedor": jogo.get("proximo_perdedor") or "",
                })

            if jogos:
                item_serie["fases"].append({
                    "id": fase,
                    "nome": FASES_AVANCO_LABELS.get(fase, fase),
                    "jogos": sorted(jogos, key=lambda j: j.get("ordem") or 999),
                })

        if item_serie["fases"]:
            saida.append(item_serie)

    return saida

def _to_int_or_none(valor):
    try:
        if valor in (None, ""):
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


def _normalizar_url_escudo_tabela(valor):
    valor = str(valor or "").strip()

    if not valor:
        return "/static/img/escudo_padrao.svg"

    if valor.startswith(("http://", "https://", "/static/", "data:")):
        return valor

    valor = valor.replace("\\", "/")

    if valor.startswith("static/"):
        return "/" + valor

    if valor.startswith("uploads/"):
        return "/static/" + valor

    if "/uploads/" in valor:
        return "/static/uploads/" + valor.split("/uploads/", 1)[1]

    return "/static/uploads/escudos/" + valor.lstrip("/")


def _mapa_escudos_equipes(equipes):
    """Monta mapa de escudos usando nome atual e login da equipe.

    O nome da equipe pode mudar pelo organizador. Por isso, sempre que o dado
    estiver disponível, o login também entra como chave estável. Mantemos as
    chaves por nome para compatibilidade com partidas/grupos antigos que ainda
    guardam texto no lugar de login.
    """
    mapa = {}

    for equipe in equipes or []:
        nome = str(
            equipe.get("nome")
            or equipe.get("equipe")
            or equipe.get("nome_equipe")
            or ""
        ).strip()
        login = str(equipe.get("login") or equipe.get("equipe_login") or "").strip()

        escudo = (
            equipe.get("escudo")
            or equipe.get("escudo_url")
            or equipe.get("logo")
            or equipe.get("imagem")
            or ""
        )
        escudo_url = _normalizar_url_escudo_tabela(escudo)

        for chave in (nome, login):
            chave = str(chave or "").strip()
            if not chave:
                continue
            mapa[chave] = escudo_url
            mapa[chave.lower()] = escudo_url
            mapa[chave.upper()] = escudo_url

    return mapa


def _buscar_escudo_mapa(mapa_escudos, nome_equipe):
    nome = str(nome_equipe or "").strip()
    if not nome:
        return _normalizar_url_escudo_tabela("")
    return (
        (mapa_escudos or {}).get(nome)
        or (mapa_escudos or {}).get(nome.lower())
        or (mapa_escudos or {}).get(nome.upper())
        or _normalizar_url_escudo_tabela("")
    )


def _quadra_label(item):
    """Texto visual de quadra, sem usar nome de grupo como fallback.

    Antes o fallback em `nome` fazia Grupo A aparecer como Quadra padrão A.
    Agora grupo sem quadra_id/quadra_nome aparece corretamente como Sem quadra.
    """
    if not item:
        return "Sem quadra"

    for campo in ("quadra_label", "quadra_exibicao", "quadra_nome"):
        valor = str((item or {}).get(campo) or "").strip()
        if valor:
            return valor

    # Para partidas antigas, o campo legacy `quadra` pode conter texto. Se for só número/id,
    # deixamos sem exibir até ser normalizado pelo banco.
    valor_legacy = str((item or {}).get("quadra") or "").strip()
    if valor_legacy and not valor_legacy.isdigit():
        return valor_legacy

    return "Sem quadra"


def _quadra_id_do_grupo(grupo):
    return _to_int_or_none((grupo or {}).get("quadra_id"))


def _quadra_padrao_do_grupo(grupos_raw, grupo_nome):
    grupo_nome = (grupo_nome or "").strip().upper()
    for g in grupos_raw or []:
        if (g.get("nome") or "").strip().upper() == grupo_nome:
            return _quadra_id_do_grupo(g)
    return None


def _dados_quadra(nome_competicao, quadra_id):
    quadra_id = _to_int_or_none(quadra_id)
    if not quadra_id:
        return None, ""
    quadra = buscar_quadra_competicao_por_id(nome_competicao, quadra_id)
    if not quadra:
        return None, ""
    return int(quadra["id"]), formatar_quadra_exibicao(quadra)


def _quadra_label_por_id(nome_competicao, quadra_id):
    quadra_id, quadra_label = _dados_quadra(nome_competicao, quadra_id)
    return quadra_label or "Sem quadra"


def _status_texto(valor):
    return str(valor or "").strip().lower().replace("-", "_")


STATUS_FINALIZADO = {
    "finalizada",
    "finalizado",
    "encerrado",
    "encerrada",
    "partida_encerrada",
}

STATUS_AO_VIVO = {
    "ao_vivo",
    "ao vivo",
    "em_andamento",
    "em andamento",
    "andamento",
    "iniciada",
    "iniciado",
}

STATUS_PRE_JOGO = {
    "pre_jogo",
    "pré_jogo",
    "pre jogo",
    "pré jogo",
}

STATUS_AGUARDANDO = {
    "aguardando",
    "agendada",
    "agendado",
    "pendente",
}


def _partida_tem_flag_finalizada(partida):
    """Finalizado sempre tem prioridade máxima sobre qualquer status ao vivo.

    Em algumas telas o jogo pode continuar com status/status_jogo antigo como
    em_andamento, mesmo depois de ter sido encerrado pelo apontador. Por isso
    verificamos todos os campos possíveis antes de classificar como AO VIVO.
    """
    if not partida:
        return False

    for campo in (
        "status",
        "status_jogo",
        "fase_partida",
        "situacao",
        "estado",
        "estado_jogo",
    ):
        if _status_texto(partida.get(campo)) in STATUS_FINALIZADO:
            return True

    for campo in ("finalizada", "partida_encerrada", "encerrada"):
        valor = partida.get(campo)
        if isinstance(valor, bool) and valor:
            return True
        if isinstance(valor, (int, float)) and int(valor) == 1:
            return True
        if isinstance(valor, str) and valor.strip().lower() in {"1", "true", "sim", "yes", "on"}:
            return True

    return bool(partida.get("finalizado_em") or partida.get("encerrado_em"))


def _status_tabela_para_trava(partida):
    """Status consolidado da tabela.

    A prioridade correta é:
    1. finalizada/encerrado;
    2. ao vivo/em andamento;
    3. pré-jogo/agendada.

    Isso evita o erro em que uma partida encerrada pelo apontador continuava
    aparecendo como AO VIVO na tabela ou no visualizador público porque algum
    campo antigo ainda estava salvo como em_andamento.
    """
    if _partida_tem_flag_finalizada(partida):
        return "finalizada"

    status = _status_texto(partida.get("status"))
    fase_partida = _status_texto(partida.get("fase_partida"))
    status_jogo = _status_texto(partida.get("status_jogo"))

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_AO_VIVO:
            return valor

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_PRE_JOGO:
            return "pre_jogo"

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_AGUARDANDO:
            return "aguardando"

    for valor in (status, fase_partida, status_jogo):
        if valor:
            return valor

    return "aguardando"


def _partida_conta_como_iniciada_para_trava(partida):
    """
    Só trava edição/exclusão quando o jogo realmente saiu do estado inicial.

    IMPORTANTE:
    No banco antigo, algumas partidas novas aparecem com status/status_jogo = pre_jogo
    mesmo sem ninguém ter aberto o pré-jogo. Por isso pre_jogo sozinho NÃO bloqueia.
    A partida só conta como iniciada quando houver sinal real de jogo: placar, sets,
    status ao vivo/finalizado, fase ao vivo/finalizada ou campo de início preenchido.
    """
    status = _status_tabela_para_trava(partida)

    if status in STATUS_FINALIZADO or status in STATUS_AO_VIVO:
        return True

    if partida.get("pre_jogo_iniciado_em") or partida.get("jogo_iniciado_em") or partida.get("finalizado_em") or partida.get("encerrado_em"):
        return True

    for campo in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"):
        try:
            if int(partida.get(campo) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass

    return False


def _fase_tem_jogo_realmente_iniciado(competicao_nome, fase_banco):
    fase_banco = (fase_banco or "grupos").strip().lower()

    for partida in listar_partidas(competicao_nome):
        fase_partida = _fase_partida_normalizada(partida)

        if fase_banco == "semifinal":
            mesma_fase = fase_partida in {"semifinal", "semifinais"}
        else:
            mesma_fase = fase_partida == fase_banco

        if mesma_fase and _partida_conta_como_iniciada_para_trava(partida):
            return True

    return False


def _fase_pode_ser_alterada_sem_travar_mata_mata(competicao_nome, fase_banco):
    """
    Regra correta:
    - Grupos/classificatórias travam quando algum jogo classificatório REALMENTE inicia.
    - Quartas, semifinal e final NÃO dependem do fim das classificatórias.
    - Criar uma partida agendada/pendente no mata-mata NÃO pode bloquear a fase.
    - Mata-mata só trava quando um jogo da própria fase vai para pré-jogo, ao vivo ou finalizado.
    """
    fase_banco = (fase_banco or "grupos").strip().lower()
    return not _fase_tem_jogo_realmente_iniciado(competicao_nome, fase_banco)



def _criar_partida_para_tabela(competicao_nome, grupo, equipe_a, equipe_b, ordem, fase_banco, origem="manual", quadra_id=None):
    """
    Cria partida pela tela da tabela.

    - Grupos usam a função padrão do banco, porque a classificatória deve respeitar o travamento estrutural.
    - Mata-mata faz INSERT direto para NÃO ser bloqueado pela classificatória travada.

    Também grava status_jogo='aguardando', porque no banco antigo status_jogo tem DEFAULT antigo 'pre_jogo'
    e isso fazia a tela achar que a partida já tinha iniciado logo depois de criar.
    """
    quadra_id, quadra_nome = _dados_quadra(competicao_nome, quadra_id)

    if fase_banco == "grupos":
        retorno = criar_partida(
            competicao_nome,
            grupo,
            equipe_a,
            equipe_b,
            ordem,
            quadra=str(quadra_id) if quadra_id else None,
            fase=fase_banco,
            origem=origem,
            quadra_id=quadra_id,
            quadra_nome=quadra_nome,
        )
        return retorno is not False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO partidas (
                    competicao, grupo, equipe_a, equipe_b, fase, ordem,
                    quadra, quadra_id, quadra_nome, origem, status, status_jogo, fase_partida
                )
                VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, 'aguardando', 'aguardando', 'aguardando')
            """, (competicao_nome, equipe_a, equipe_b, fase_banco, ordem, str(quadra_id) if quadra_id else None, quadra_id, quadra_nome or '', origem))
        conn.commit()

    return True


def _mapa_quadras_formatadas(nome_competicao):
    """Busca as quadras uma única vez e monta cache id -> nome formatado.

    A geração automática pode criar dezenas/centenas de jogos. Chamar
    buscar_quadra_competicao_por_id() para cada jogo faz uma consulta ao Neon
    por partida e deixa a geração muito lenta. Este mapa evita esse gargalo.
    """
    mapa = {}
    try:
        for q in listar_quadras_competicao(nome_competicao) or []:
            qid = _to_int_or_none(q.get("id"))
            if qid:
                mapa[qid] = formatar_quadra_exibicao(q)
    except Exception as e:
        print("AVISO _mapa_quadras_formatadas:", repr(e))
    return mapa


def _quadra_nome_cache(mapa_quadras, quadra_id):
    quadra_id = _to_int_or_none(quadra_id)
    if not quadra_id:
        return None, ""
    return quadra_id, (mapa_quadras or {}).get(quadra_id, "")


def _inserir_partidas_em_lote(partidas):
    """Insere partidas em lote com um único roundtrip/commit.

    Substitui o fluxo antigo que chamava criar_partida() para cada jogo.
    Esse fluxo antigo abria validações/consultas/commits repetidos e era o
    principal motivo da geração automática demorar minutos.

    Status inicial correto: AGUARDANDO. A partida só vira PRE_JOGO quando o
    apontador realmente assumir/abrir a conferência.
    """
    partidas = [p for p in (partidas or []) if p]
    if not partidas:
        return 0

    colunas_partidas = _buscar_colunas_tabela("partidas") or set()

    campos_base = [
        "competicao", "grupo", "equipe_a", "equipe_b", "fase", "ordem",
        "quadra", "quadra_id", "quadra_nome", "origem", "rodada", "data_hora", "status",
    ]
    extras_possiveis = [
        "status_jogo", "fase_partida", "status_operacao",
        "sets_a", "sets_b", "pontos_a", "pontos_b",
    ]

    # Compatibilidade com bancos antigos: só insere colunas que realmente existem.
    campos = [c for c in campos_base if c in colunas_partidas]
    campos.extend([c for c in extras_possiveis if c in colunas_partidas and c not in campos])

    valores = []
    for p in partidas:
        quadra_id = _to_int_or_none(p.get("quadra_id"))
        mapa_valores = {
            "competicao": p.get("competicao"),
            "grupo": p.get("grupo"),
            "equipe_a": p.get("equipe_a"),
            "equipe_b": p.get("equipe_b"),
            "fase": p.get("fase") or "grupos",
            "ordem": int(p.get("ordem") or 0),
            "quadra": str(quadra_id) if quadra_id else None,
            "quadra_id": quadra_id,
            "quadra_nome": p.get("quadra_nome") or "",
            "origem": p.get("origem") or "automatica",
            "rodada": p.get("rodada"),
            "data_hora": p.get("data_hora"),
            "status": "aguardando",
            "status_jogo": "aguardando",
            "fase_partida": "aguardando",
            "status_operacao": "livre",
            "sets_a": 0,
            "sets_b": 0,
            "pontos_a": 0,
            "pontos_b": 0,
        }
        valores.append(tuple(mapa_valores.get(c) for c in campos))

    placeholders = ", ".join(["%s"] * len(campos))

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO partidas ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                valores,
            )
        conn.commit()

    return len(valores)

def _fase_partida_normalizada(partida):
    fase = (
        partida.get("fase")
        or partida.get("fase_partida")
        or "grupos"
    )
    fase = str(fase).strip().lower()

    if fase in {"classificatoria", "classificatorias", "grupo", "grupos"}:
        return "grupos"
    if "quarta" in fase:
        return "quartas"
    if "semi" in fase:
        return "semifinal"
    if "terceiro" in fase or "3" in fase and "lugar" in fase:
        return "terceiro_lugar"
    if "final" in fase:
        return "final"

    return fase or "grupos"

def _filtrar_partidas_por_fase(partidas, fase_subaba):
    fase_subaba = (fase_subaba or "classificatorias").strip().lower()

    def mesma_fase(partida):
        fase = _fase_partida_normalizada(partida)

        if fase_subaba == "classificatorias":
            return fase == "grupos"
        if fase_subaba == "quartas":
            return fase == "quartas"
        if fase_subaba in {"oitavas"}:
            return fase == "oitavas"
        if fase_subaba in {"semifinais", "semifinal"}:
            return fase in {"semifinal", "semifinais"}
        if fase_subaba in {"finais", "final"}:
            return fase == "final"
        if fase_subaba == "terceiro_lugar":
            return fase == "terceiro_lugar"

        return False

    return [p for p in partidas if mesma_fase(p)]


def _status_normalizado(partida):
    return _status_tabela_para_trava(partida)


def _status_exibicao(partida):
    status = _status_normalizado(partida)

    mapa = {
        "pre_jogo": "PRÉ-JOGO",
        "aguardando": "AGUARDANDO",
        "agendada": "AGUARDANDO",
        "em andamento": "AO VIVO",
        "ao vivo": "AO VIVO",
        "ao_vivo": "AO VIVO",
        "andamento": "AO VIVO",
        "em_andamento": "AO VIVO",
        "finalizada": "FINALIZADO",
        "finalizado": "FINALIZADO",
        "encerrado": "FINALIZADO",
        "encerrada": "FINALIZADO",
    }

    return mapa.get(status, (status or "AGUARDANDO").replace("_", " ").upper())


def _partida_esta_finalizada(partida):
    return _partida_tem_flag_finalizada(partida) or _status_normalizado(partida) in STATUS_FINALIZADO


def _partida_esta_ao_vivo(partida):
    """Reconhece jogo ao vivo mesmo quando algum status legado não foi atualizado.

    O apontador salva os pontos continuamente, mas em alguns fluxos o campo
    status/status_jogo pode continuar como pre_jogo. Para o visualizador público,
    uma partida não finalizada com placar, sets ou marca real de início deve ser
    tratada como AO VIVO.
    """
    if _partida_esta_finalizada(partida):
        return False

    if _status_normalizado(partida) in STATUS_AO_VIVO:
        return True

    if partida.get("jogo_iniciado_em") or partida.get("pre_jogo_iniciado_em"):
        return True

    for campo in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"):
        try:
            if int((partida or {}).get(campo) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass

    return False


def _montar_parciais(partida):
    parciais = []

    for i in range(1, 6):
        a = partida.get(f"set{i}_a")
        b = partida.get(f"set{i}_b")

        if a is not None and b is not None:
            try:
                parciais.append(f"{int(a)}x{int(b)}")
            except (TypeError, ValueError):
                parciais.append(f"{a}x{b}")

    return " / ".join(parciais) if parciais else "-"


def _preparar_partidas(partidas, mapa_escudos=None, competicao=None):
    partidas_preparadas = []

    for p in partidas:
        partida = dict(p)

        partida["fase_normalizada"] = _fase_partida_normalizada(partida)
        partida["status_normalizado"] = _status_normalizado(partida)
        partida["status_exibicao"] = _status_exibicao(partida)
        partida["ao_vivo"] = _partida_esta_ao_vivo(partida)
        partida["finalizada"] = _partida_esta_finalizada(partida)
        partida["parciais_formatadas"] = _montar_parciais(partida)
        partida["pode_excluir"] = not _partida_conta_como_iniciada_para_trava(partida)

        partida["placar_ao_vivo_a"] = int(
            partida.get("pontos_a")
            or partida.get("placar_a")
            or 0
        )

        partida["placar_ao_vivo_b"] = int(
            partida.get("pontos_b")
            or partida.get("placar_b")
            or 0
        )

        aplicar_placar_exibicao_partida(partida, competicao or {})

        # Para jogo ao vivo, o campo de consulta/visualização deve mostrar
        # os pontos do set atual. O placar_exibicao em M3/M5 representa sets
        # e por isso fica 0x0 no início da partida.
        if partida.get("ao_vivo") and not partida.get("finalizada"):
            partida["placar_ao_vivo_a"] = int(partida.get("pontos_a") or partida.get("placar_a") or 0)
            partida["placar_ao_vivo_b"] = int(partida.get("pontos_b") or partida.get("placar_b") or 0)
            partida["placar_ao_vivo"] = f'{partida["placar_ao_vivo_a"]} x {partida["placar_ao_vivo_b"]}'

        partida["quadra_label"] = _quadra_label(partida)
        partida["quadra_id_normalizado"] = _to_int_or_none(partida.get("quadra_id"))
        partida["data_hora_valor"] = str(partida.get("data_hora") or "").strip()
        _dh = partida["data_hora_valor"].replace(" ", "T")
        if len(_dh) >= 16:
            partida["data_hora_input"] = _dh[:16]
            data_p, hora_p = _dh[:10], _dh[11:16]
            partida["data_hora_label"] = f"{data_p[8:10]}/{data_p[5:7]}/{data_p[0:4]} {hora_p}"
        else:
            partida["data_hora_input"] = _dh
            partida["data_hora_label"] = _dh

        partida["escudo_a"] = _normalizar_url_escudo_tabela(partida.get("escudo_a")) if partida.get("escudo_a") else _buscar_escudo_mapa(mapa_escudos, partida.get("equipe_a"))
        partida["escudo_b"] = _normalizar_url_escudo_tabela(partida.get("escudo_b")) if partida.get("escudo_b") else _buscar_escudo_mapa(mapa_escudos, partida.get("equipe_b"))
        partida["equipe_a_escudo"] = partida["escudo_a"]
        partida["equipe_b_escudo"] = partida["escudo_b"]

        partidas_preparadas.append(partida)

    return sorted(
        partidas_preparadas,
        key=lambda p: (
            p.get("data_hora_valor") or "9999-12-31 23:59",
            p.get("quadra_label") or "",
            p.get("ordem") or 0,
        )
    )
    

def _to_bool(valor):
    if isinstance(valor, bool):
        return valor
    if valor is None:
        return False
    return str(valor).strip().lower() in {"1", "true", "sim", "yes", "on"}


def _valor_inteiro_regra(competicao, chaves, padrao):
    for chave in chaves:
        valor = competicao.get(chave)
        if valor not in (None, ""):
            try:
                return int(valor)
            except (TypeError, ValueError):
                pass
    return padrao


def _bool_por_chaves(competicao, chaves):
    for chave in chaves:
        if chave in competicao:
            return _to_bool(competicao.get(chave))
    return False


CRITERIOS_CLASSIFICACAO_PADRAO = [
    "pontos",
    "vitorias",
    "saldo_sets",
    "sets_average",
    "saldo_pontos",
    "pontos_average",
    "confronto_direto",
    "sorteio",
]

CRITERIOS_CLASSIFICACAO_SUPORTADOS = {
    "pontos",
    "vitorias",
    "sets_average",
    "pontos_average",
    "saldo_sets",
    "saldo_pontos",
    "sets_pro",
    "sets_contra",
    "pontos_pro",
    "pontos_contra",
    "confronto_direto",
    "coef_sets",
    "coef_pontos",
    "fair_play",
    "menor_wo",
    "sorteio",
}

CRITERIOS_MENOR_MELHOR = {"sets_contra", "pontos_contra", "fair_play", "menor_wo"}


CRITERIOS_CLASSIFICACAO_COLUNAS = {
    "pontos": {"campo": "pontos", "titulo": "P"},
    "vitorias": {"campo": "vitorias", "titulo": "V"},
    "derrotas": {"campo": "derrotas", "titulo": "D"},
    "jogos": {"campo": "jogos", "titulo": "J"},
    "saldo_sets": {"campo": "saldo_sets", "titulo": "DS"},
    "sets_average": {"campo": "sets_average_exibicao", "titulo": "SA"},
    "coef_sets": {"campo": "sets_average_exibicao", "titulo": "SA"},
    "saldo_pontos": {"campo": "saldo_pontos", "titulo": "DP"},
    "pontos_average": {"campo": "pontos_average_exibicao", "titulo": "PA"},
    "coef_pontos": {"campo": "pontos_average_exibicao", "titulo": "PA"},
    "sets_pro": {"campo": "sets_pro", "titulo": "SP"},
    "sets_contra": {"campo": "sets_contra", "titulo": "SC"},
    "pontos_pro": {"campo": "pontos_pro", "titulo": "PF"},
    "pontos_contra": {"campo": "pontos_contra", "titulo": "PC"},
    "fair_play": {"campo": "fair_play", "titulo": "FP"},
    "menor_wo": {"campo": "wo", "titulo": "WO"},
}

COLUNAS_PUBLICAS_SET_UNICO = [
    {"campo": "pontos", "titulo": "P", "descricao": "Pontos na classificação"},
    {"campo": "jogos", "titulo": "J", "descricao": "Jogos disputados"},
    {"campo": "vitorias", "titulo": "V", "descricao": "Vitórias"},
    {"campo": "derrotas", "titulo": "D", "descricao": "Derrotas"},
    {"campo": "pontos_average_exibicao", "titulo": "PA", "descricao": "Pontos average: PF dividido por PC"},
    {"campo": "saldo_pontos", "titulo": "DP", "descricao": "Diferença de pontos: PF menos PC"},
    {"campo": "pontos_pro", "titulo": "PF", "descricao": "Pontos feitos"},
    {"campo": "pontos_contra", "titulo": "PC", "descricao": "Pontos cedidos"},
]

COLUNAS_PUBLICAS_SETS = [
    {"campo": "pontos", "titulo": "P", "descricao": "Pontos na classificação"},
    {"campo": "jogos", "titulo": "J", "descricao": "Jogos disputados"},
    {"campo": "vitorias", "titulo": "V", "descricao": "Vitórias"},
    {"campo": "derrotas", "titulo": "D", "descricao": "Derrotas"},
    {"campo": "sets_pro", "titulo": "SP", "descricao": "Sets pró"},
    {"campo": "sets_contra", "titulo": "SC", "descricao": "Sets contra"},
    {"campo": "saldo_sets", "titulo": "DS", "descricao": "Diferença de sets: SP menos SC"},
    {"campo": "sets_average_exibicao", "titulo": "SA", "descricao": "Sets average: SP dividido por SC"},
    {"campo": "pontos_average_exibicao", "titulo": "PA", "descricao": "Pontos average: PF dividido por PC"},
    {"campo": "saldo_pontos", "titulo": "DP", "descricao": "Diferença de pontos: PF menos PC"},
    {"campo": "pontos_pro", "titulo": "PF", "descricao": "Pontos feitos"},
    {"campo": "pontos_contra", "titulo": "PC", "descricao": "Pontos cedidos"},
]


def _formatar_numero_decimal(valor):
    try:
        valor = float(valor or 0)
    except (TypeError, ValueError):
        valor = 0.0

    if valor == float("inf"):
        return "∞"

    texto = f"{valor:.3f}".rstrip("0").rstrip(".")
    return texto or "0"


def _calcular_sets_average_valor(sets_pro, sets_contra):
    """Calcula sets average pelo acumulado da equipe.

    Regra técnica adotada no sistema:
    - enquanto a equipe não sofreu sets, usa divisor 0.5;
    - depois que sofreu pelo menos 1 set, usa o valor real acumulado.
    """
    try:
        sets_pro = int(sets_pro or 0)
    except (TypeError, ValueError):
        sets_pro = 0

    try:
        sets_contra = int(sets_contra or 0)
    except (TypeError, ValueError):
        sets_contra = 0

    if sets_pro <= 0:
        return 0.0

    if sets_contra <= 0:
        return float("inf")

    return sets_pro / sets_contra


def _calcular_pontos_average_valor(pontos_pro, pontos_contra):
    """Calcula pontos average pelo acumulado da equipe.

    Regra técnica adotada no sistema:
    - enquanto a equipe não sofreu pontos, usa divisor 1;
    - depois que sofreu pelo menos 1 ponto, usa o valor real acumulado.
    """
    try:
        pontos_pro = int(pontos_pro or 0)
    except (TypeError, ValueError):
        pontos_pro = 0

    try:
        pontos_contra = int(pontos_contra or 0)
    except (TypeError, ValueError):
        pontos_contra = 0

    if pontos_pro <= 0:
        return 0.0

    if pontos_contra <= 0:
        return float("inf")

    return pontos_pro / pontos_contra


def _formatar_sets_average_exibicao(sets_pro, sets_contra):
    return _formatar_numero_decimal(_calcular_sets_average_valor(sets_pro, sets_contra))


def _formatar_pontos_average_exibicao(pontos_pro, pontos_contra):
    return _formatar_numero_decimal(_calcular_pontos_average_valor(pontos_pro, pontos_contra))


def _criterios_efetivos_ate_sorteio(criterios):
    criterios = list(criterios or [])
    if "sorteio" in criterios:
        return criterios[:criterios.index("sorteio") + 1]
    return criterios


def _competicao_eh_set_unico_tabela(competicao):
    competicao = competicao or {}
    texto = " ".join(
        str(competicao.get(chave) or "")
        for chave in ("sets_tipo", "tipo_sets", "formato_sets", "melhor_de")
    ).strip().lower().replace("-", "_").replace(" ", "_")

    return texto in {"set_unico", "único", "unico", "1_set", "melhor_de_1", "md1", "1"} or "set_unico" in texto


def _colunas_classificacao_publica(competicao):
    """Colunas exibidas no link público.

    A exibição é independente da ordem de desempate. A classificação continua
    sendo ordenada por _aplicar_criterios_classificacao usando os critérios
    configurados pelo organizador.
    """
    colunas = COLUNAS_PUBLICAS_SET_UNICO if _competicao_eh_set_unico_tabela(competicao) else COLUNAS_PUBLICAS_SETS
    return [dict(c) for c in colunas]


def _colunas_classificacao_por_criterios(criterios):
    """Compatibilidade com telas antigas que exibem apenas critérios ativos."""
    colunas = []
    vistos = set()

    for criterio in _criterios_efetivos_ate_sorteio(criterios):
        cfg = CRITERIOS_CLASSIFICACAO_COLUNAS.get(criterio)
        if not cfg:
            continue

        campo = cfg["campo"]
        if campo in vistos:
            continue

        colunas.append({
            "criterio": criterio,
            "campo": campo,
            "titulo": cfg["titulo"],
            "descricao": cfg.get("descricao", cfg["titulo"]),
        })
        vistos.add(campo)

    if not colunas:
        colunas.append({"criterio": "pontos", "campo": "pontos", "titulo": "P", "descricao": "Pontos"})

    return colunas


def _normalizar_criterios_classificacao(valor):
    """
    Lê a ordem salva em competicoes.criterios_desempate.

    A coluna antiga foi mantida por compatibilidade, mas agora ela representa
    a ORDEM DOS CRITÉRIOS DE CLASSIFICAÇÃO. Ex.:
    pontos,vitorias,saldo_sets,confronto_direto,saldo_pontos,sorteio
    """
    if isinstance(valor, (list, tuple)):
        brutos = valor
    else:
        texto = str(valor or "").strip()
        if texto.startswith("["):
            try:
                import json
                carregado = json.loads(texto)
                brutos = carregado if isinstance(carregado, list) else []
            except Exception:
                brutos = []
        else:
            brutos = texto.split(",")

    criterios = []
    vistos = set()

    aliases = {
        "vitórias": "vitorias",
        "vitorias": "vitorias",
        "pontos average": "pontos_average",
        "sets average": "sets_average",
        "saldo de sets": "saldo_sets",
        "saldo de pontos": "saldo_pontos",
        "confronto": "confronto_direto",
        "confronto direto": "confronto_direto",
        "wo": "menor_wo",
        "menor numero de wo": "menor_wo",
        "menor número de w.o.": "menor_wo",
    }

    for item in brutos:
        criterio = str(item or "").strip().lower()
        criterio = criterio.replace("-", "_").replace(" ", "_")
        criterio = aliases.get(criterio, criterio)

        if criterio in CRITERIOS_CLASSIFICACAO_SUPORTADOS and criterio not in vistos:
            criterios.append(criterio)
            vistos.add(criterio)

    if not criterios:
        criterios = list(CRITERIOS_CLASSIFICACAO_PADRAO)

    # Não corta os critérios abaixo do sorteio.
    # O sorteio encerra o desempate apenas no momento do cálculo, dentro de
    # _aplicar_criterios_classificacao. Assim a tela continua podendo salvar
    # e reordenar todos os critérios escolhidos pelo organizador.
    return criterios


def _sets_para_vitoria_classificacao(competicao):
    """Define quantos sets o vencedor precisa fazer conforme a regra da competição."""
    texto = " ".join(
        str(competicao.get(chave) or "")
        for chave in ("sets_tipo", "tipo_sets", "formato_sets", "melhor_de")
    ).strip().lower()

    if "5" in texto or "cinco" in texto:
        return 3

    if "unico" in texto or "único" in texto or "1" in texto:
        return 1

    return 2


def _resultado_foi_tiebreak(sets_vencedor, sets_perdedor, competicao):
    sets_para_vitoria = _sets_para_vitoria_classificacao(competicao)

    if sets_para_vitoria <= 1:
        return False

    return int(sets_vencedor or 0) == sets_para_vitoria and int(sets_perdedor or 0) == (sets_para_vitoria - 1)


def _obter_regras_classificacao(competicao):
    criterios = _normalizar_criterios_classificacao(
        competicao.get("criterios_desempate")
        or competicao.get("criterios_classificacao")
        or ""
    )

    return {
        "pontos_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_vitoria", "vitoria_set_unico", "vitoria_2x0", "vitoria_3x0"],
            2
        ),
        "pontos_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_derrota", "derrota_set_unico", "derrota_0x2", "derrota_0x3"],
            0
        ),
        "pontos_tiebreak_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_vitoria", "vitoria_tiebreak", "vitoria_2x1", "vitoria_3x2"],
            2
        ),
        "pontos_tiebreak_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_derrota", "derrota_tiebreak", "derrota_1x2", "derrota_2x3"],
            1
        ),
        "criterios": criterios,
    }


def _valor_criterio(linha, nome):
    if nome == "pontos":
        return linha.get("pontos", 0)

    if nome == "vitorias":
        return linha.get("vitorias", 0)

    if nome in {"sets_average", "coef_sets"}:
        return linha.get(
            "sets_average_valor",
            _calcular_sets_average_valor(linha.get("sets_pro", 0), linha.get("sets_contra", 0))
        )

    if nome in {"pontos_average", "coef_pontos"}:
        return linha.get(
            "pontos_average_valor",
            _calcular_pontos_average_valor(linha.get("pontos_pro", 0), linha.get("pontos_contra", 0))
        )

    if nome == "saldo_sets":
        return linha.get("saldo_sets", 0)

    if nome == "saldo_pontos":
        return linha.get("saldo_pontos", 0)

    if nome == "sets_pro":
        return linha.get("sets_pro", 0)

    if nome == "sets_contra":
        return linha.get("sets_contra", 0)

    if nome == "pontos_pro":
        return linha.get("pontos_pro", 0)

    if nome == "pontos_contra":
        return linha.get("pontos_contra", 0)

    if nome == "fair_play":
        return linha.get("fair_play", 0)

    if nome == "menor_wo":
        return linha.get("wo", linha.get("wos", 0))

    return 0


def _valor_ordenacao_criterio(linha, criterio):
    valor = _valor_criterio(linha, criterio)
    if criterio in CRITERIOS_MENOR_MELHOR:
        try:
            return -float(valor)
        except (TypeError, ValueError):
            return 0
    return valor


def _resolver_confronto_direto(bloco, partidas, grupo):
    if len(bloco) <= 1:
        return bloco

    nomes = [l["equipe"] for l in bloco]
    mini = {
        nome: {
            "pontos": 0,
            "saldo_sets": 0,
            "pontos_pro": 0,
            "pontos_contra": 0,
            "saldo_pontos": 0,
            "vitorias": 0,
        }
        for nome in nomes
    }

    # Otimização: quando receber dict, já vem indexado por grupo e só varre
    # partidas daquele grupo. Antes varria TODAS as partidas dentro de cada
    # bloco de empate, o que fazia a classificação ficar muito lenta.
    if isinstance(partidas, dict):
        partidas_iter = partidas.get(grupo) or []
    else:
        partidas_iter = [p for p in (partidas or []) if p.get("grupo") == grupo and _partida_esta_finalizada(p)]

    for p in partidas_iter:
        a = p.get("equipe_a")
        b = p.get("equipe_b")

        if a not in mini or b not in mini:
            continue

        try:
            sets_a = int(p.get("sets_a") or 0)
        except (TypeError, ValueError):
            sets_a = 0

        try:
            sets_b = int(p.get("sets_b") or 0)
        except (TypeError, ValueError):
            sets_b = 0

        if sets_a == sets_b:
            continue

        mini[a]["saldo_sets"] += sets_a - sets_b
        mini[b]["saldo_sets"] += sets_b - sets_a

        pontos_a = 0
        pontos_b = 0
        for i in range(1, 6):
            sa = p.get(f"set{i}_a")
            sb = p.get(f"set{i}_b")
            if sa is not None and sb is not None:
                try:
                    pontos_a += int(sa)
                    pontos_b += int(sb)
                except (TypeError, ValueError):
                    pass

        mini[a]["pontos_pro"] += pontos_a
        mini[a]["pontos_contra"] += pontos_b
        mini[b]["pontos_pro"] += pontos_b
        mini[b]["pontos_contra"] += pontos_a
        mini[a]["saldo_pontos"] = mini[a]["pontos_pro"] - mini[a]["pontos_contra"]
        mini[b]["saldo_pontos"] = mini[b]["pontos_pro"] - mini[b]["pontos_contra"]

        if sets_a > sets_b:
            mini[a]["pontos"] += 1
            mini[a]["vitorias"] += 1
        else:
            mini[b]["pontos"] += 1
            mini[b]["vitorias"] += 1

    return sorted(
        bloco,
        key=lambda linha: (
            mini[linha["equipe"]]["pontos"],
            mini[linha["equipe"]]["vitorias"],
            mini[linha["equipe"]]["saldo_sets"],
            mini[linha["equipe"]]["saldo_pontos"],
            mini[linha["equipe"]]["pontos_pro"],
        ),
        reverse=True
    )


def _aplicar_criterios_classificacao(linhas, partidas, grupo, criterios):
    """
    Aplica a classificação exatamente na ordem cadastrada pelo organizador.
    Cada critério só mexe dentro de blocos que ainda estão empatados no critério anterior.
    """
    if not linhas:
        return linhas

    def aplicar_bloco(bloco, indice_criterio):
        if len(bloco) <= 1 or indice_criterio >= len(criterios):
            return bloco

        criterio = criterios[indice_criterio]

        if criterio == "sorteio":
            bloco = list(bloco)
            random.shuffle(bloco)
            return bloco

        if criterio == "confronto_direto":
            ordenado = _resolver_confronto_direto(bloco, partidas, grupo)
            # Depois do confronto direto, segue para os próximos critérios apenas nos empates técnicos restantes.
            return aplicar_bloco(ordenado, indice_criterio + 1)

        ordenado = sorted(
            bloco,
            key=lambda linha: _valor_ordenacao_criterio(linha, criterio),
            reverse=True,
        )

        resultado = []
        pos = 0
        while pos < len(ordenado):
            atual = ordenado[pos]
            valor_atual = _valor_ordenacao_criterio(atual, criterio)
            sub_bloco = [atual]
            prox = pos + 1

            while prox < len(ordenado) and _valor_ordenacao_criterio(ordenado[prox], criterio) == valor_atual:
                sub_bloco.append(ordenado[prox])
                prox += 1

            resultado.extend(aplicar_bloco(sub_bloco, indice_criterio + 1))
            pos = prox

        return resultado

    return aplicar_bloco(list(linhas), 0)


# Compatibilidade com chamadas antigas.
def _aplicar_desempates_profissional(linhas, partidas, grupo, criterios):
    return _aplicar_criterios_classificacao(linhas, partidas, grupo, criterios)


def _calcular_classificacao(partidas, grupos, competicao, mapa_escudos=None):
    regras = _obter_regras_classificacao(competicao)
    classificacao = {}

    for g in grupos:
        nome_grupo = g["grupo"]["nome"]
        classificacao[nome_grupo] = []

        equipes_ordenadas = sorted(
            g["equipes"],
            key=lambda e: (e.get("equipe") or "").lower()
        )

        for e in equipes_ordenadas:
            classificacao[nome_grupo].append({
                "equipe": e["equipe"],
                "escudo": _buscar_escudo_mapa(mapa_escudos, e.get("equipe")),
                "jogos": 0,
                "vitorias": 0,
                "derrotas": 0,
                "sets_pro": 0,
                "sets_contra": 0,
                "saldo_sets": 0,
                "pontos_pro": 0,
                "pontos_contra": 0,
                "saldo_pontos": 0,
                "pontos": 0,
                "wo": 0,
            })

    mapa = {
        grupo: {linha["equipe"]: linha for linha in linhas}
        for grupo, linhas in classificacao.items()
    }

    for p in partidas:
        if not _partida_esta_finalizada(p):
            continue

        grupo = p.get("grupo")
        equipe_a = p.get("equipe_a")
        equipe_b = p.get("equipe_b")

        if not grupo or grupo not in mapa:
            continue
        if equipe_a not in mapa[grupo] or equipe_b not in mapa[grupo]:
            continue

        try:
            sets_a = int(p.get("sets_a") or 0)
        except (TypeError, ValueError):
            sets_a = 0

        try:
            sets_b = int(p.get("sets_b") or 0)
        except (TypeError, ValueError):
            sets_b = 0

        if sets_a == sets_b:
            continue

        linha_a = mapa[grupo][equipe_a]
        linha_b = mapa[grupo][equipe_b]

        linha_a["jogos"] += 1
        linha_b["jogos"] += 1

        linha_a["sets_pro"] += sets_a
        linha_a["sets_contra"] += sets_b
        linha_b["sets_pro"] += sets_b
        linha_b["sets_contra"] += sets_a

        pontos_a = 0
        pontos_b = 0

        for i in range(1, 6):
            sa = p.get(f"set{i}_a")
            sb = p.get(f"set{i}_b")
            if sa is not None and sb is not None:
                try:
                    pontos_a += int(sa)
                    pontos_b += int(sb)
                except (TypeError, ValueError):
                    pass

        linha_a["pontos_pro"] += pontos_a
        linha_a["pontos_contra"] += pontos_b
        linha_b["pontos_pro"] += pontos_b
        linha_b["pontos_contra"] += pontos_a

        tipo_encerramento = str(p.get("tipo_encerramento") or "").strip().lower()
        origem_resultado = str(p.get("origem_resultado") or "").strip().lower()
        if tipo_encerramento in {"wo", "w.o.", "w.o"} or origem_resultado == "wo":
            if sets_a > sets_b:
                linha_b["wo"] = int(linha_b.get("wo") or 0) + 1
            elif sets_b > sets_a:
                linha_a["wo"] = int(linha_a.get("wo") or 0) + 1

        if sets_a > sets_b:
            linha_a["vitorias"] += 1
            linha_b["derrotas"] += 1

            if _resultado_foi_tiebreak(sets_a, sets_b, competicao):
                linha_a["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_b["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_a["pontos"] += regras["pontos_vitoria"]
                linha_b["pontos"] += regras["pontos_derrota"]
        else:
            linha_b["vitorias"] += 1
            linha_a["derrotas"] += 1

            if _resultado_foi_tiebreak(sets_b, sets_a, competicao):
                linha_b["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_a["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_b["pontos"] += regras["pontos_vitoria"]
                linha_a["pontos"] += regras["pontos_derrota"]

    for grupo, linhas in classificacao.items():
        for linha in linhas:
            linha["saldo_sets"] = linha["sets_pro"] - linha["sets_contra"]
            linha["saldo_pontos"] = linha["pontos_pro"] - linha["pontos_contra"]
            linha["sets_average_valor"] = _calcular_sets_average_valor(linha["sets_pro"], linha["sets_contra"])
            linha["pontos_average_valor"] = _calcular_pontos_average_valor(linha["pontos_pro"], linha["pontos_contra"])
            linha["sets_average_exibicao"] = _formatar_numero_decimal(linha["sets_average_valor"])
            linha["pontos_average_exibicao"] = _formatar_numero_decimal(linha["pontos_average_valor"])
            linha.setdefault("fair_play", 0)
            linha.setdefault("wo", 0)

    criterios_ativos = regras.get("criterios") or list(CRITERIOS_CLASSIFICACAO_PADRAO)
    partidas_por_grupo = _partidas_finalizadas_por_grupo(partidas)

    for grupo, linhas in classificacao.items():
        classificacao[grupo] = _aplicar_criterios_classificacao(
            linhas,
            partidas_por_grupo,
            grupo,
            criterios_ativos,
        )

    return classificacao



def _normalizar_cache_classificacao(valor_cache, assinatura_atual=None):
    """Extrai a classificação salva no cache sem depender de um formato único.

    O banco já teve mais de uma versão dessa função de cache. Por isso este
    helper aceita dict, JSON em texto ou tupla/lista e só usa o cache quando
    ele realmente contém uma classificação válida.
    """
    if not valor_cache:
        return None

    if isinstance(valor_cache, str):
        try:
            valor_cache = json.loads(valor_cache)
        except Exception:
            return None

    if isinstance(valor_cache, (list, tuple)):
        # Compatibilidade com retornos antigos: (classificacao, assinatura) ou
        # (assinatura, classificacao). Preferimos o item que parece dict/list.
        candidatos = list(valor_cache)
        for item in candidatos:
            normalizado = _normalizar_cache_classificacao(item, assinatura_atual)
            if normalizado:
                return normalizado
        return None

    if not isinstance(valor_cache, dict):
        return None

    assinatura_cache = valor_cache.get("assinatura") or valor_cache.get("hash") or valor_cache.get("checksum")
    if assinatura_atual and assinatura_cache and str(assinatura_cache) != str(assinatura_atual):
        return None

    classificacao = (
        valor_cache.get("classificacao")
        or valor_cache.get("dados")
        or valor_cache.get("valor")
        or valor_cache.get("cache")
    )

    if isinstance(classificacao, str):
        try:
            classificacao = json.loads(classificacao)
        except Exception:
            return None

    return classificacao if isinstance(classificacao, dict) else None


def _assinatura_classificacao_segura(competicao_nome, partidas_preparadas, grupos, competicao):
    """Gera assinatura sem bater no banco.

    Isso remove uma consulta pesada que antes rodava na abertura da tabela e na
    geração de mata-mata. Se algo der errado, retorna None e a classificação é
    calculada normalmente, sem cache.
    """
    try:
        return _assinatura_classificacao_local(competicao_nome, partidas_preparadas, grupos, competicao)
    except Exception as e:
        print("AVISO classificacao/assinatura_local:", repr(e))
        return None


def _obter_cache_classificacao_seguro(competicao_nome, assinatura):
    try:
        return obter_cache_classificacao(competicao_nome, assinatura)
    except TypeError:
        try:
            return obter_cache_classificacao(competicao_nome)
        except Exception as e:
            print("AVISO classificacao/obter_cache:", repr(e))
    except Exception as e:
        print("AVISO classificacao/obter_cache:", repr(e))
    return None


def _salvar_cache_classificacao_seguro(competicao_nome, assinatura, classificacao):
    try:
        salvar_cache_classificacao(competicao_nome, assinatura, classificacao)
        return
    except TypeError:
        pass
    except Exception as e:
        print("AVISO classificacao/salvar_cache:", repr(e))
        return

    tentativas = [
        (competicao_nome, classificacao, assinatura),
        (competicao_nome, {"assinatura": assinatura, "classificacao": classificacao}),
        (competicao_nome, classificacao),
    ]
    for args in tentativas:
        try:
            salvar_cache_classificacao(*args)
            return
        except TypeError:
            continue
        except Exception as e:
            print("AVISO classificacao/salvar_cache:", repr(e))
            return


def _calcular_ou_obter_classificacao_cacheada(competicao_nome, partidas_preparadas, grupos, competicao, mapa_escudos=None):
    """Usa cache de classificação quando possível e calcula como fallback.

    Esta função estava sendo chamada pelo visualizador público e pela aba de
    classificação, mas não existia no arquivo. Sem ela, a rota pública quebrava
    com NameError e retornava 500. A implementação abaixo é defensiva: qualquer
    problema no cache apenas recalcula a classificação, sem derrubar a tela.
    """
    assinatura = _assinatura_classificacao_segura(competicao_nome, partidas_preparadas, grupos, competicao)

    if assinatura:
        cache_bruto = _obter_cache_classificacao_seguro(competicao_nome, assinatura)
        classificacao_cache = _normalizar_cache_classificacao(cache_bruto, assinatura)
        if classificacao_cache:
            return classificacao_cache, True

    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)

    if assinatura:
        _salvar_cache_classificacao_seguro(competicao_nome, assinatura, classificacao)

    return classificacao, False




def _bool_publico(valor):
    if valor is True or valor == 1:
        return True
    if valor is False or valor is None:
        return False
    return str(valor).strip().lower() in {"1", "true", "sim", "yes", "on", "avancado", "avançado"}


def _modo_scout_ativo_publico(partida, competicao):
    modo = str((partida or {}).get("modo_operacao") or (competicao or {}).get("modo_operacao") or "simples").strip().lower()
    return modo in {"avancado", "avançado", "scout", "com_scout"} or _bool_publico((partida or {}).get("scout_ativo"))


def _evento_detalhes_publico(ev):
    raw = ev.get("detalhes") if isinstance(ev, dict) else None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            dado = json.loads(raw)
            return dado if isinstance(dado, dict) else {}
        except Exception:
            return {}
    return {}


def _lado_para_nome_publico(partida, lado):
    lado = str(lado or "").strip().upper()
    if lado == "A":
        return partida.get("equipe_a_operacional") or partida.get("equipe_a") or "Equipe A"
    if lado == "B":
        return partida.get("equipe_b_operacional") or partida.get("equipe_b") or "Equipe B"
    return "Equipe"


def _lado_pontuador_evento_publico(ev):
    detalhes = _evento_detalhes_publico(ev)
    lado = str(
        detalhes.get("equipe_pontuadora")
        or detalhes.get("equipe_ponto")
        or detalhes.get("lado_ponto")
        or ""
    ).strip().upper()
    if lado in {"A", "B"}:
        return lado
    tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
    resultado = str(ev.get("resultado") or detalhes.get("resultado") or detalhes.get("tipo_lance") or "").strip().lower()
    equipe = str(ev.get("equipe") or "").strip().upper()
    if equipe in {"A", "B"}:
        if resultado in {"erro", "falta"}:
            return "B" if equipe == "A" else "A"
        if tipo in {"ponto", "retardamento_penalidade"}:
            return equipe
    return ""


def _normalizar_acao_publica(valor):
    texto = str(valor or "").strip().lower().replace("_", " ").replace("-", " ")
    trocas = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e", "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u", "ç": "c",
    }
    for origem, destino in trocas.items():
        texto = texto.replace(origem, destino)
    return " ".join(texto.split())


def _evento_eh_acao_negativa_adversario_publico(ev):
    """Identifica lances em que o ponto nasceu de erro/falta do adversário.

    Nesses eventos a equipe pontuadora recebe o ponto, mas não é quem executou
    a ação exibida. A autoria pertence ao lado oposto.
    """
    detalhes = _evento_detalhes_publico(ev)
    valores = [
        ev.get("fundamento"),
        ev.get("resultado"),
        ev.get("detalhe"),
        ev.get("tipo"),
        detalhes.get("fundamento"),
        detalhes.get("resultado"),
        detalhes.get("tipo_lance"),
        detalhes.get("tipo_erro"),
        detalhes.get("detalhe_lance"),
        detalhes.get("detalhe"),
    ]
    texto = " | ".join(_normalizar_acao_publica(v) for v in valores if v not in (None, ""))
    marcadores = (
        "erro de saque", "erro saque", "saque errado",
        "erro geral", "erro", "falta",
        "erro de rotacao", "erro rotacao", "rotacao",
        "invasao", "conducao", "dois toques",
    )
    return any(marcador in texto for marcador in marcadores)


def _lado_responsavel_evento_publico(ev, lado_ponto):
    """Retorna quem executou a ação, e não apenas quem recebeu o ponto.

    Para ataque, ace e bloqueio, o responsável normalmente é o pontuador.
    Para erro de saque, erro geral, falta, rotação, invasão, condução e dois
    toques, o responsável é obrigatoriamente o adversário do pontuador.

    Alguns eventos antigos gravaram equipe_responsavel com o mesmo lado que
    recebeu o ponto. Por isso a regra negativa tem prioridade sobre esse campo.
    """
    if _evento_eh_acao_negativa_adversario_publico(ev) and lado_ponto in {"A", "B"}:
        return "B" if lado_ponto == "A" else "A"

    detalhes = _evento_detalhes_publico(ev)
    lado_explicito = str(
        detalhes.get("equipe_responsavel")
        or detalhes.get("lado_responsavel")
        or detalhes.get("equipe_autora")
        or detalhes.get("lado_acao")
        or ""
    ).strip().upper()
    if lado_explicito in {"A", "B"}:
        return lado_explicito

    return lado_ponto


def _rotulo_fundamento_publico(valor):
    txt_normalizado = _normalizar_acao_publica(valor)
    mapa = {
        "ataque": "Ataque",
        "bloqueio": "Bloqueio",
        "saque": "Saque",
        "ace": "Ace",
        "erro": "Erro geral",
        "erro geral": "Erro geral",
        "erro saque": "Erro de saque",
        "erro de saque": "Erro de saque",
        "erro rotacao": "Erro de rotação",
        "erro de rotacao": "Erro de rotação",
        "rotacao": "Erro de rotação",
        "falta": "Falta",
        "invasao": "Invasão",
        "conducao": "Condução",
        "dois toques": "Dois toques",
        "levantamento": "Levantamento",
        "defesa": "Defesa",
        "recepcao": "Recepção",
    }
    if txt_normalizado in mapa:
        return mapa[txt_normalizado]
    return str(valor or "Ponto").strip().replace("_", " ").title() or "Ponto"


def _descricao_evento_publico(ev, partida, scout_ativo):
    detalhes = _evento_detalhes_publico(ev)
    tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
    fundamento = ev.get("fundamento") or detalhes.get("fundamento") or detalhes.get("detalhe_lance") or detalhes.get("tipo_erro")
    resultado = ev.get("resultado") or detalhes.get("resultado") or detalhes.get("tipo_lance")
    detalhe = ev.get("detalhe") or detalhes.get("detalhe") or detalhes.get("detalhe_lance")
    numero = ev.get("numero") or detalhes.get("atleta_numero") or detalhes.get("numero")
    atleta = ev.get("atleta_nome") or detalhes.get("atleta_nome") or ""
    lado_ponto = _lado_pontuador_evento_publico(ev)
    equipe_ponto = _lado_para_nome_publico(partida, lado_ponto)
    lado_responsavel = _lado_responsavel_evento_publico(ev, lado_ponto)
    equipe_responsavel = _lado_para_nome_publico(partida, lado_responsavel)
    acao_negativa = _evento_eh_acao_negativa_adversario_publico(ev)

    if tipo not in {"ponto", "retardamento_penalidade"}:
        base = tipo.replace("_", " ").title() if tipo else "Evento"
        if detalhe:
            base += f" • {detalhe}"
        return base

    if not scout_ativo:
        return f"Ponto para {equipe_ponto}"

    acao = _rotulo_fundamento_publico(fundamento or resultado or tipo)
    pessoa = ""
    if numero and atleta:
        pessoa = f"#{numero} {atleta}"
    elif numero:
        pessoa = f"#{numero}"
    elif atleta:
        pessoa = atleta

    if acao_negativa:
        if pessoa:
            return f"{acao} de {pessoa} ({equipe_responsavel}) — ponto para {equipe_ponto}"
        if detalhe and _normalizar_acao_publica(detalhe) not in {_normalizar_acao_publica(acao)}:
            return f"{acao} • {detalhe} ({equipe_responsavel}) — ponto para {equipe_ponto}"
        return f"{acao} da {equipe_responsavel} — ponto para {equipe_ponto}"

    if pessoa:
        return f"{acao} de {pessoa} ({equipe_responsavel})"
    if detalhe and str(detalhe).strip().lower() not in {str(acao).lower()}:
        return f"{acao} • {detalhe} ({equipe_responsavel})"
    return f"{acao} ({equipe_responsavel})"


def _montar_linha_ponto_publico(partida, eventos, scout_ativo):
    eventos_ordenados = list(reversed(eventos or []))
    placares_por_set = {}
    linhas = []
    stats = {}

    for ev in eventos_ordenados:
        tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        if tipo not in {"ponto", "retardamento_penalidade"}:
            continue
        set_num = int(ev.get("set_numero") or 1)
        atual = placares_por_set.setdefault(set_num, {"a": 0, "b": 0})
        lado = _lado_pontuador_evento_publico(ev)
        if lado == "A":
            atual["a"] += 1
        elif lado == "B":
            atual["b"] += 1
        else:
            continue

        detalhes = _evento_detalhes_publico(ev)
        fundamento = _rotulo_fundamento_publico(ev.get("fundamento") or detalhes.get("fundamento") or detalhes.get("detalhe_lance") or detalhes.get("tipo_erro") or ev.get("resultado"))
        lado_responsavel = _lado_responsavel_evento_publico(ev, lado)
        equipe_nome = _lado_para_nome_publico(partida, lado_responsavel)
        stats.setdefault(equipe_nome, {})
        stats[equipe_nome][fundamento] = stats[equipe_nome].get(fundamento, 0) + 1

        linhas.append({
            "id": ev.get("id"),
            "set": set_num,
            "placar_a": atual["a"],
            "placar_b": atual["b"],
            "placar": f'{atual["a"]} x {atual["b"]}',
            "lado_ponto": lado,
            "equipe_ponto": _lado_para_nome_publico(partida, lado),
            "equipe_responsavel": equipe_nome,
            "descricao": _descricao_evento_publico(ev, partida, scout_ativo),
            "fundamento": fundamento,
            "numero": ev.get("numero") or detalhes.get("atleta_numero") or detalhes.get("numero") or "",
            "atleta_nome": ev.get("atleta_nome") or detalhes.get("atleta_nome") or "",
        })

    linhas.sort(key=lambda x: (x["set"], x["id"] or 0), reverse=True)
    evolucao_sets = []
    for set_num in sorted(placares_por_set.keys()):
        pontos = [{"placar": "0 x 0", "a": 0, "b": 0}]
        a = b = 0
        for linha in reversed([l for l in linhas if l["set"] == set_num]):
            a = linha["placar_a"]
            b = linha["placar_b"]
            pontos.append({"placar": f"{a} x {b}", "a": a, "b": b})
        evolucao_sets.append({"set": set_num, "pontos": pontos})

    return linhas, evolucao_sets, stats


def _buscar_destaque_partida_publico(partida_id, competicao):
    try:
        criar_tabela_destaques_partida()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        d.lado,
                        d.atleta_id,
                        d.numero,
                        d.nome,
                        d.observacao,
                        d.equipe,
                        d.criado_em,
                        COALESCE(
                            NULLIF(a.foto_atleta, ''),
                            (
                                SELECT NULLIF(a2.foto_atleta, '')
                                FROM atletas a2
                                WHERE a2.competicao = d.competicao
                                  AND LOWER(TRIM(COALESCE(a2.equipe, ''))) = LOWER(TRIM(COALESCE(d.equipe, '')))
                                  AND (
                                      (d.numero IS NOT NULL AND a2.numero = d.numero)
                                      OR LOWER(TRIM(COALESCE(a2.nome, ''))) = LOWER(TRIM(COALESCE(d.nome, '')))
                                  )
                                  AND COALESCE(a2.foto_atleta, '') <> ''
                                ORDER BY CASE WHEN d.numero IS NOT NULL AND a2.numero = d.numero THEN 0 ELSE 1 END, a2.id DESC
                                LIMIT 1
                            )
                        ) AS foto_atleta
                    FROM destaques_partida d
                    LEFT JOIN atletas a ON a.id = d.atleta_id
                    WHERE d.partida_id = %s AND d.competicao = %s
                    ORDER BY d.id DESC
                    LIMIT 1
                """, (partida_id, competicao))
                return cur.fetchone()
    except Exception as e:
        print("AVISO visualizador/destaque_partida:", repr(e), flush=True)
        return None


def _contexto_partida_publica(competicao_nome, partida_id):
    competicao = buscar_competicao_por_nome(competicao_nome) or {"nome": competicao_nome}
    partida = buscar_partida_por_id(partida_id, competicao_nome)
    if not partida:
        return None
    mapa_escudos = {
        partida.get("equipe_a"): partida.get("escudo_a"),
        partida.get("equipe_b"): partida.get("escudo_b"),
    }
    preparada = (_preparar_partidas([partida], mapa_escudos, competicao) or [partida])[0]
    estado = None
    # O apontador mantém o estado corrente em memória para responder sem
    # bloquear no Neon. O visualizador público deve ler esse mesmo estado vivo;
    # consultar apenas o banco faz o placar ficar atrasado até o próximo
    # checkpoint (fim de set/sincronização periódica).
    estado_vivo = {}
    try:
        candidato = obter_estado_cache(partida_id) or {}
        competicao_cache = str(candidato.get("competicao") or "").strip()
        if not competicao_cache or competicao_cache == str(competicao_nome or "").strip():
            estado_vivo = candidato
    except Exception as e:
        print("AVISO visualizador/estado_vivo:", repr(e), flush=True)

    # O banco permanece como fallback para recarga/reinício do processo. Quando
    # existe estado vivo, ele sempre prevalece sobre a fotografia persistida.
    estado_banco = {}
    if not estado_vivo:
        try:
            estado_banco = buscar_estado_jogo_partida(partida_id, competicao_nome) or {}
        except Exception:
            estado_banco = {}

    estado = dict(estado_banco)
    estado.update(estado_vivo)
    eventos = listar_eventos_partida(partida_id, competicao_nome, limite=600) or []
    scout_ativo = _modo_scout_ativo_publico(partida, competicao)
    timeline, evolucao_sets, stats = _montar_linha_ponto_publico(partida, eventos, scout_ativo)
    destaque = _buscar_destaque_partida_publico(partida_id, competicao_nome)
    return {
        "competicao": competicao,
        "partida": preparada,
        "estado": estado,
        "scout_ativo": scout_ativo,
        "timeline": timeline,
        "evolucao_sets": evolucao_sets,
        "stats": stats,
        "destaque": destaque,
    }


# =========================================================
# VISUALIZADOR PÚBLICO
# =========================================================
@tabela_bp.route("/visualizador/<competicao_nome>")
def visualizador_publico(competicao_nome):
    try:
        normalizar_vinculos_quadras_competicao(competicao_nome)
    except Exception as e:
        print("AVISO visualizador/normalizar_quadras:", repr(e))

    grupos_raw = _listar_grupos_cache(competicao_nome)
    # Visualizador público também precisa de partidas frescas para abrir já com o placar atual.
    partidas = listar_partidas(competicao_nome) or []
    equipes_competicao = _listar_equipes_competicao_cache(competicao_nome)
    mapa_escudos = _mapa_escudos_equipes(equipes_competicao)

    grupos = _grupos_com_equipes_cacheados(competicao_nome, grupos_raw)

    competicao = buscar_competicao_por_nome(competicao_nome) or {
        "nome": competicao_nome
    }

    partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
    classificacao, classificacao_do_cache = _calcular_ou_obter_classificacao_cacheada(competicao_nome, partidas_preparadas, grupos, competicao, mapa_escudos)
    regras_classificacao = _obter_regras_classificacao(competicao)
    criterios_classificacao = _criterios_efetivos_ate_sorteio(regras_classificacao.get("criterios"))
    colunas_classificacao = _colunas_classificacao_publica(competicao)
    set_unico = _competicao_eh_set_unico_tabela(competicao)
    avanco = _avanco_cache(competicao_nome)
    status_avanco = _status_avanco_cache(competicao_nome)
    avanco_gerado = _avanco_gerado_cache(competicao_nome)
    status_avanco["gerado"] = avanco_gerado
    if not avanco_gerado:
        partidas_preparadas = [p for p in partidas_preparadas if not _partida_eh_avanco(p)]
    # No visualizador público, o chaveamento só deve existir depois do
    # encerramento completo das classificatórias e da geração oficial dos jogos.
    # Antes disso, não mostramos placeholders nem antecipamos cruzamentos.
    exibir_avanco_publico = bool(status_avanco.get("fechada")) and bool(avanco_gerado)
    avanco_espelho = (
        _montar_espelho_avanco(avanco, partidas_preparadas, True)
        if exibir_avanco_publico
        else []
    )

    # Organização exclusiva do visualizador público: rodada -> data/hora ->
    # ordem definida pelo organizador. Não altera a ordem usada nas telas
    # administrativas nem na geração das partidas.
    partidas_preparadas = sorted(
        partidas_preparadas,
        key=lambda p: (
            int(p.get("rodada") or 999999),
            p.get("data_hora_valor") or "9999-12-31 23:59",
            int(p.get("ordem") or 0),
            p.get("quadra_label") or "",
            int(p.get("id") or 0),
        ),
    )

    codigo_publico = garantir_codigo_publico_competicao(competicao_nome)
    partidas_ao_vivo = [
        p for p in partidas_preparadas
        if bool(p.get("ao_vivo")) and not bool(p.get("finalizada"))
    ]

    return render_template(
        "visualizador_publico.html",
        competicao_nome=competicao_nome,
        codigo_publico=codigo_publico,
        grupos=grupos,
        classificacao=classificacao,
        partidas=partidas_preparadas,
        partidas_ao_vivo=partidas_ao_vivo,
        criterios_classificacao=criterios_classificacao,
        colunas_classificacao=colunas_classificacao,
        set_unico=set_unico,
        avanco=avanco,
        avanco_status=status_avanco,
        avanco_espelho=avanco_espelho,
        fase_labels=FASES_AVANCO_LABELS,
    )


@tabela_bp.route("/visualizador/<competicao_nome>/ao-vivo/dados")
def visualizador_publico_ao_vivo_dados(competicao_nome):
    """Lista leve dos jogos ao vivo para fazer o destaque aparecer sozinho."""
    competicao = buscar_competicao_por_nome(competicao_nome) or {"nome": competicao_nome}
    partidas = listar_partidas(competicao_nome) or []
    preparadas = _preparar_partidas(partidas, {}, competicao)
    ids = sorted(
        int(p.get("id")) for p in preparadas
        if p.get("id") and p.get("ao_vivo") and not p.get("finalizada")
    )
    resposta = jsonify({"ok": True, "partidas_ao_vivo": ids})
    resposta.headers["Cache-Control"] = "no-store, max-age=0"
    return resposta


@tabela_bp.route("/visualizador/<competicao_nome>/partida/<int:partida_id>")
def visualizador_publico_partida(competicao_nome, partida_id):
    contexto = _contexto_partida_publica(competicao_nome, partida_id)
    if not contexto:
        return "Partida não encontrada.", 404
    codigo_publico = garantir_codigo_publico_competicao(competicao_nome)
    return render_template("visualizador_partida_publica.html", competicao_nome=competicao_nome, codigo_publico=codigo_publico, **contexto)


@tabela_bp.route("/visualizador/<competicao_nome>/partida/<int:partida_id>/dados")
def visualizador_publico_partida_dados(competicao_nome, partida_id):
    """Estado leve para atualização frequente do placar público.

    Não carrega eventos, evolução, scout, fotos nem escudos em base64. Isso
    mantém cada polling pequeno e evita saturar o Render/Neon quando várias
    pessoas acompanham a mesma partida.
    """
    competicao = buscar_competicao_por_nome(competicao_nome) or {"nome": competicao_nome}
    partida = buscar_partida_por_id(partida_id, competicao_nome)
    if not partida:
        return jsonify({"ok": False, "erro": "Partida não encontrada."}), 404

    # Prioriza o mesmo estado vivo usado pelo apontador. Isso evita consultar
    # o Neon a cada atualização pública e mantém o placar correto mesmo durante
    # uma breve instabilidade do banco.
    try:
        estado = obter_estado_cache(partida_id) or {}
    except Exception:
        estado = {}
    if not estado:
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao_nome) or {}
        except Exception:
            estado = {}

    mapa_escudos = {
        partida.get("equipe_a"): partida.get("escudo_a"),
        partida.get("equipe_b"): partida.get("escudo_b"),
    }
    p = (_preparar_partidas([partida], mapa_escudos, competicao) or [partida])[0]

    eventos_versao = 0
    try:
        estado_versao = obter_estado_versao(partida_id)
    except Exception:
        estado_versao = 0
    destaque_versao = 0
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(MAX(id), 0) AS versao
                    FROM eventos
                    WHERE partida_id = %s AND competicao = %s
                """, (partida_id, competicao_nome))
                row = cur.fetchone() or {}
                eventos_versao = int(row.get("versao") or 0)
                try:
                    cur.execute("""
                        SELECT COALESCE(MAX(id), 0) AS versao
                        FROM destaques_partida
                        WHERE partida_id = %s AND competicao = %s
                    """, (partida_id, competicao_nome))
                    row = cur.fetchone() or {}
                    destaque_versao = int(row.get("versao") or 0)
                except Exception:
                    destaque_versao = 0
    except Exception as e:
        print("AVISO visualizador/dados_leves_versao:", repr(e), flush=True)

    resposta = jsonify({
        "ok": True,
        "partida": {
            "id": p.get("id"),
            "equipe_a": p.get("equipe_a"),
            "equipe_b": p.get("equipe_b"),
            "status_exibicao": p.get("status_exibicao"),
            "ao_vivo": bool(p.get("ao_vivo")),
            "finalizada": bool(p.get("finalizada")),
            "set_unico": bool(p.get("set_unico")),
            "sets_a": int(estado.get("sets_a") or p.get("sets_a") or 0),
            "sets_b": int(estado.get("sets_b") or p.get("sets_b") or 0),
            "set_atual": int(estado.get("set_atual") or p.get("set_atual") or 1),
            "pontos_a": int(estado.get("pontos_a") or estado.get("placar_a") or p.get("placar_exibicao_a") or 0),
            "pontos_b": int(estado.get("pontos_b") or estado.get("placar_b") or p.get("placar_exibicao_b") or 0),
            "parciais_formatadas": p.get("parciais_formatadas") or "",
        },
        # O banco informa a versão persistida; estado_versao muda em cada ação
        # local e faz o cliente buscar detalhes logo após o ponto.
        "eventos_versao": eventos_versao,
        "estado_versao": estado_versao,
        "ultima_acao": estado.get("ultima_acao") or "",
        "destaque_versao": destaque_versao,
    })
    resposta.headers["Cache-Control"] = "no-store, max-age=0"
    return resposta


@tabela_bp.route("/visualizador/<competicao_nome>/partida/<int:partida_id>/dados/detalhes")
def visualizador_publico_partida_detalhes(competicao_nome, partida_id):
    """Dados completos, consultados apenas quando eventos/destaque mudam."""
    contexto = _contexto_partida_publica(competicao_nome, partida_id)
    if not contexto:
        return jsonify({"ok": False, "erro": "Partida não encontrada."}), 404
    resposta = jsonify({
        "ok": True,
        "scout_ativo": bool(contexto.get("scout_ativo")),
        "timeline": contexto.get("timeline") or [],
        "evolucao_sets": contexto.get("evolucao_sets") or [],
        "stats": contexto.get("stats") or {},
        "destaque": contexto.get("destaque") or None,
    })
    resposta.headers["Cache-Control"] = "no-store, max-age=0"
    return resposta


# =========================================================
# ROTAS PÚBLICAS CURTAS
# =========================================================
@tabela_bp.route("/v/<codigo_publico>")
def visualizador_publico_curto(codigo_publico):
    competicao = buscar_competicao_por_codigo_publico(codigo_publico)
    if not competicao:
        return "Competição não encontrada.", 404
    # Renderiza a mesma tela sem redirecionar; a URL curta permanece no navegador.
    return visualizador_publico(competicao.get("nome"))


@tabela_bp.route("/v/<codigo_publico>/ao-vivo/dados")
def visualizador_publico_ao_vivo_dados_curta(codigo_publico):
    competicao = buscar_competicao_por_codigo_publico(codigo_publico)
    if not competicao:
        return jsonify({"ok": False, "erro": "Competição não encontrada."}), 404
    return visualizador_publico_ao_vivo_dados(competicao.get("nome"))


@tabela_bp.route("/v/<codigo_publico>/partida/<int:partida_id>")
def visualizador_publico_partida_curta(codigo_publico, partida_id):
    competicao = buscar_competicao_por_codigo_publico(codigo_publico)
    if not competicao:
        return "Competição não encontrada.", 404
    competicao_nome = competicao.get("nome")
    contexto = _contexto_partida_publica(competicao_nome, partida_id)
    if not contexto:
        return "Partida não encontrada.", 404
    return render_template(
        "visualizador_partida_publica.html",
        competicao_nome=competicao_nome,
        codigo_publico=str(codigo_publico or "").upper(),
        **contexto,
    )


@tabela_bp.route("/v/<codigo_publico>/partida/<int:partida_id>/dados")
def visualizador_publico_partida_dados_curta(codigo_publico, partida_id):
    competicao = buscar_competicao_por_codigo_publico(codigo_publico)
    if not competicao:
        return jsonify({"ok": False, "erro": "Competição não encontrada."}), 404
    return visualizador_publico_partida_dados(competicao.get("nome"), partida_id)


@tabela_bp.route("/v/<codigo_publico>/partida/<int:partida_id>/dados/detalhes")
def visualizador_publico_partida_detalhes_curta(codigo_publico, partida_id):
    competicao = buscar_competicao_por_codigo_publico(codigo_publico)
    if not competicao:
        return jsonify({"ok": False, "erro": "Competição não encontrada."}), 404
    return visualizador_publico_partida_detalhes(competicao.get("nome"), partida_id)


def _estrutura_grupo_unico(competicao):
    """Retorna True quando a competição deve operar como grupo único.

    Considera qtd_grupos = 1 ou tem_grupos desligado como grupo único para a tela
    não pedir a mesma configuração novamente na Tabela.
    """
    try:
        qtd = int((competicao or {}).get("qtd_grupos") or 0)
    except Exception:
        qtd = 0
    tem_grupos = _to_bool((competicao or {}).get("tem_grupos"))
    return qtd <= 1 or not tem_grupos


def _nomes_grupos_automaticos(qtd):
    try:
        qtd = int(qtd or 1)
    except Exception:
        qtd = 1
    qtd = max(1, min(qtd, 26))
    return [chr(ord("A") + i) for i in range(qtd)]


def _qtd_grupos_configurada(competicao):
    try:
        qtd = int((competicao or {}).get("qtd_grupos") or 0)
    except Exception:
        qtd = 0
    if _estrutura_grupo_unico(competicao):
        return 1
    return max(2, min(qtd or 2, 26))



def _existe_distribuicao_salva_fora_do_grupo_a(nome_competicao):
    """Protege sorteios/manuais já salvos.

    A tela da Tabela pode ser aberta várias vezes e não pode redistribuir times
    automaticamente. Se já existir equipe em grupo B/C/D..., não sincronizamos
    tudo para o Grupo A só porque algum salvamento parcial deixou qtd_grupos=1.
    """
    try:
        grupos = listar_grupos(nome_competicao) or []
        for g in grupos:
            nome_g = str(g.get("nome") or "").strip().upper()
            if nome_g in {"", "A"}:
                continue
            equipes = listar_equipes_por_grupo(g.get("id")) or []
            if any(str(e.get("equipe") or "").strip() for e in equipes):
                return True
    except Exception as e:
        print("AVISO existe_distribuicao_salva_fora_do_grupo_a:", repr(e), flush=True)
    return False


def _garantir_grupos_da_estrutura(competicao):
    """Garante que a aba Tabela obedeça a estrutura salva na competição.

    - Grupo único: cria/usa apenas o grupo A e coloca todas as equipes nele.
    - 2+ grupos: cria automaticamente A, B, C, D... conforme qtd_grupos,
      mas NÃO distribui equipes sozinho. O organizador pode preencher manualmente
      ou usar o sorteio.
    """
    if not competicao:
        return False
    nome_competicao = competicao.get("nome")
    if not nome_competicao or fase_grupos_esta_travada_por_jogo(nome_competicao):
        return False

    if _estrutura_grupo_unico(competicao):
        # Não sobrescreve sorteio/distribuição manual já salva.
        # Só sincroniza Grupo A quando realmente não há equipes salvas em outros grupos.
        if _existe_distribuicao_salva_fora_do_grupo_a(nome_competicao):
            return False
        return _sincronizar_grupo_unico_automatico(competicao)

    qtd = _qtd_grupos_configurada(competicao)
    nomes_estrutura = _nomes_grupos_automaticos(qtd)
    existentes = listar_grupos(nome_competicao) or []
    existentes_nomes = {str(g.get("nome") or "").strip().upper() for g in existentes}
    mudou = False

    # Cria automaticamente somente os grupos definidos na Estrutura da competição.
    # Ex.: qtd_grupos=4 => A, B, C e D. A tela não deve depender de criação manual.
    for nome_grupo in nomes_estrutura:
        if nome_grupo not in existentes_nomes:
            if criar_grupo(nome_grupo, nome_competicao) is not False:
                mudou = True

    # Grupos extras antigos ficam preservados se tiverem vínculo/partida, mas a tela
    # passa a trabalhar prioritariamente com os grupos da estrutura. Não apagamos aqui
    # para não destruir histórico por acidente.
    if mudou:
        _limpar_cache_tabela(nome_competicao)
    return mudou


def _limpar_vinculos_equipes_grupos_competicao(nome_competicao):
    if not nome_competicao:
        return 0
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM grupos_equipes WHERE competicao = %s", (nome_competicao,))
            total = cur.rowcount or 0
        conn.commit()
    return total


def _sortear_equipes_nos_grupos(competicao):
    """Distribui equipes aleatoriamente e balanceado nos grupos configurados."""
    if not competicao:
        return {"ok": False, "mensagem": "Competição não encontrada."}
    nome_competicao = competicao.get("nome")
    if not nome_competicao:
        return {"ok": False, "mensagem": "Competição sem nome."}
    if fase_grupos_esta_travada_por_jogo(nome_competicao):
        return {"ok": False, "mensagem": "A fase classificatória já iniciou. Não é possível sortear grupos."}
    if _estrutura_grupo_unico(competicao):
        _sincronizar_grupo_unico_automatico(competicao)
        return {"ok": True, "mensagem": "Competição em grupo único: todas as equipes ficam no Grupo A."}

    _garantir_grupos_da_estrutura(competicao)
    qtd = _qtd_grupos_configurada(competicao)
    grupos = listar_grupos(nome_competicao) or []
    grupos_alvo = []
    nomes_alvo = set(_nomes_grupos_automaticos(qtd))
    for g in grupos:
        nome_g = str(g.get("nome") or "").strip().upper()
        if nome_g in nomes_alvo:
            grupos_alvo.append(g)
    grupos_alvo = sorted(grupos_alvo, key=lambda g: str(g.get("nome") or ""))

    equipes = listar_equipes_da_competicao(nome_competicao) or []
    nomes_equipes = []
    vistos = set()
    for eq in equipes:
        nome = str(eq.get("nome") or eq.get("equipe") or "").strip()
        chave = nome.lower()
        if nome and chave not in vistos:
            nomes_equipes.append(nome)
            vistos.add(chave)

    if len(grupos_alvo) < qtd:
        return {"ok": False, "mensagem": "Não foi possível criar todos os grupos configurados."}
    if not nomes_equipes:
        return {"ok": False, "mensagem": "Cadastre as equipes antes de sortear os grupos."}

    random.shuffle(nomes_equipes)
    _limpar_vinculos_equipes_grupos_competicao(nome_competicao)
    for idx, equipe in enumerate(nomes_equipes):
        grupo = grupos_alvo[idx % len(grupos_alvo)]
        adicionar_equipe_no_grupo(grupo.get("id"), equipe, nome_competicao)

    _limpar_cache_tabela_e_classificacao(nome_competicao)
    tamanhos = {str(g.get("nome") or "").strip().upper(): 0 for g in grupos_alvo}
    for idx, _equipe in enumerate(nomes_equipes):
        nome_g = str(grupos_alvo[idx % len(grupos_alvo)].get("nome") or "").strip().upper()
        tamanhos[nome_g] = tamanhos.get(nome_g, 0) + 1
    resumo = ", ".join(f"{g}: {qtd_eq}" for g, qtd_eq in tamanhos.items())
    return {"ok": True, "mensagem": f"Sorteio realizado: {len(nomes_equipes)} equipe(s) distribuída(s). {resumo}."}


def _sincronizar_grupo_unico_automatico(competicao):
    if not competicao or not _estrutura_grupo_unico(competicao):
        return False
    nome_competicao = competicao.get("nome")
    if not nome_competicao or fase_grupos_esta_travada_por_jogo(nome_competicao):
        return False
    grupos = listar_grupos(nome_competicao) or []
    grupo_a = next((g for g in grupos if str(g.get("nome") or "").upper() == "A"), None)
    if not grupo_a:
        criar_grupo("A", nome_competicao)
        grupos = listar_grupos(nome_competicao) or []
        grupo_a = next((g for g in grupos if str(g.get("nome") or "").upper() == "A"), None)
    if not grupo_a:
        return False
    try:
        equipes = listar_equipes_da_competicao(nome_competicao) or []
        ja = listar_equipes_por_grupo(grupo_a.get("id")) or []
        nomes_ja = {str(e.get("equipe") or "").strip().lower() for e in ja}
        for eq in equipes:
            nome = (eq.get("nome") or "").strip()
            if nome and nome.lower() not in nomes_ja:
                adicionar_equipe_no_grupo(grupo_a.get("id"), nome, nome_competicao)
        quadras = listar_quadras_competicao(nome_competicao) or []
        ativas = [q for q in quadras if q.get("ativa") is not False]
        if len(ativas) == 1:
            vincular_grupo_a_quadra(nome_competicao, "A", ativas[0].get("id"))
        _limpar_cache_tabela(nome_competicao)
        return True
    except Exception as e:
        print("AVISO sincronizar grupo único:", repr(e), flush=True)
        return False
def _injetar_acoes_grupos_estrutura_html(html, competicao):
    """Adiciona botões de grupos quando o template antigo ainda não tem a opção."""
    if not isinstance(html, str) or not competicao:
        return html
    try:
        nomes = _nomes_grupos_automaticos(_qtd_grupos_configurada(competicao))
        grupo_unico = _estrutura_grupo_unico(competicao)
    except Exception:
        return html

    if grupo_unico:
        titulo = "Grupo único"
        texto = "A estrutura está como grupo único. O sistema mantém todas as equipes no Grupo A."
        botoes = '<form method="post" action="/tabela/garantir-grupos"><button type="submit">Sincronizar Grupo A</button></form>'
    else:
        titulo = f"Grupos da competição: {', '.join(nomes)}"
        texto = "Os grupos são criados pela estrutura salva. Preencha as equipes manualmente ou use o sorteio."
        botoes = '<form method="post" action="/tabela/garantir-grupos"><button type="submit">Criar grupos da estrutura</button></form><form method="post" action="/tabela/sortear-grupos" onsubmit="return confirm(\'Sortear novamente vai refazer a distribuição das equipes nos grupos. Continuar?\')"><button type="submit">Sortear equipes nos grupos</button></form>'

    bloco = f'''
<style>
.grupos-estrutura-atalho{{margin:12px 0;padding:12px 14px;border:1px solid #bfdbfe;background:#eff6ff;border-radius:14px;color:#0f172a;display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap}}
.grupos-estrutura-atalho strong{{display:block;font-weight:900;color:#1d4ed8}}
.grupos-estrutura-atalho span{{font-size:13px;color:#334155}}
.grupos-estrutura-atalho .acoes{{display:flex;gap:8px;flex-wrap:wrap}}
.grupos-estrutura-atalho form{{margin:0}}
.grupos-estrutura-atalho button{{border:0;border-radius:999px;padding:9px 13px;font-weight:900;background:#2563eb;color:white;cursor:pointer}}
.grupos-estrutura-atalho form+form button{{background:#16a34a}}
</style>
<div class="grupos-estrutura-atalho">
  <div><strong>{titulo}</strong><span>{texto}</span></div>
  <div class="acoes">{botoes}</div>
</div>
'''

    if 'grupos-estrutura-atalho' in html:
        return html

    alvos = ['<main', '<section', '<div class="container"', '<div class="conteudo"']
    for alvo in alvos:
        idx = html.find(alvo)
        if idx >= 0:
            fim = html.find('>', idx)
            if fim >= 0:
                return html[:fim+1] + bloco + html[fim+1:]
    if '</body>' in html:
        return html.replace('</body>', bloco + '\n</body>')
    return bloco + html




def _render_tabela_resposta(contexto):
    """Renderiza a tabela inteira ou apenas o miolo quando a troca de aba vem por fetch.

    Isso permite que /tabela continue funcionando igual no navegador, mas o
    JavaScript consiga trocar Grupos/Partidas/Classificação/Link público sem
    recarregar base, menu lateral e topo inteiro.
    """
    parcial = (request.args.get("parcial") == "1") or (request.headers.get("X-Requested-With") == "fetch")
    template = "tabela/_conteudo.html" if parcial else "tabela.html"
    return render_template(template, **contexto)

# =========================================================
# TELA PRINCIPAL
# =========================================================
@tabela_bp.route("/tabela")
@exigir_organizador_da_competicao
def tabela_view():
    usuario = session.get("usuario")

    if not usuario:
        flash("Sessão expirada. Faça login novamente.", "erro")
        return redirect(url_for("painel.inicio"))

    competicao = _buscar_competicao_organizador_cache(usuario)

    if not competicao:
        flash("Nenhuma competição vinculada a este organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    _remover_flash_permissao_falso()

    aba = (request.args.get("aba") or "geracao").strip().lower()
    if aba not in {"geracao", "partidas", "classificacao", "visualizador"}:
        aba = "geracao"

    fase_subaba = _fase_subaba_canonica(request.args.get("fase") or "classificatorias")
    fases_validas = {"classificatorias", "quartas", "semifinal", "final", "oitavas", "terceiro_lugar"}
    if fase_subaba not in fases_validas:
        fase_subaba = "classificatorias"

    # Base leve: dados que a navegação superior e travas usam em qualquer aba.
    # O restante só é carregado conforme a aba ativa. Isso evita que abrir
    # "Configurações" carregue classificação, partidas e avanço completos.
    nome_competicao = competicao["nome"]
    _garantir_grupos_da_estrutura(competicao)
    fases = _fases_disponiveis(competicao)
    grupos_travados = _grupos_travados_cache(nome_competicao)
    fase_banco_ativa = _fase_subaba_para_banco(fase_subaba)
    fase_atual_travada = _fase_atual_travada_cache(nome_competicao, fase_banco_ativa)

    contexto = {
        "competicao": competicao,
        "aba_ativa": aba,
        "fase_ativa": fase_subaba,
        "fase_labels": FASES_AVANCO_LABELS,
        "competicao_travada": _competicao_travada_cache(nome_competicao),
        "grupos_travados": grupos_travados,
        "fase_atual_travada": fase_atual_travada,
        "fase_banco_ativa": fase_banco_ativa,
        "grupos": [],
        "equipes": [],
        "quadras": [],
        "partidas": [],
        "partidas_fase": [],
        "classificacao": {},
        "criterios_classificacao": [],
        "colunas_classificacao": [],
        "avanco": {},
        "avanco_status": {"gerado": False},
        "avanco_fases_tabs": [],
        "avanco_series_fase": [],
        "avanco_serie_ativa": "",
        "avanco_espelho": [],
        "config_agenda": None,
        "config_geracao": None,
        "grupo_unico_auto": _estrutura_grupo_unico(competicao),
        "quadra_unica_auto": False,
        "codigo_publico": "",
        "link_publico_path": "",
        "link_publico": "",
        **fases,
    }

    serie_param_cache = (request.args.get("serie") or "").strip().lower()
    # A aba de partidas precisa refletir placar ao vivo. Não usamos pacote cache
    # nela porque uma abertura antes do jogo começar deixava 0x0 congelado por
    # vários segundos para organizador/equipes/visualizador.
    pacote_cacheado = None if aba == "partidas" else _pacote_cache_get(nome_competicao, aba, fase_subaba, serie_param_cache)
    if pacote_cacheado is not None:
        contexto.update(pacote_cacheado)
        html = _render_tabela_resposta(contexto)
        if aba == "geracao":
            html = _injetar_acoes_grupos_estrutura_html(html, competicao)
        return html

    pacote_contexto = {}

    # Aba Configurações: carrega apenas grupos, equipes, quadras e agenda.
    if aba == "geracao":
        quadras = _quadras_cache(nome_competicao, competicao.get("qtd_quadras") or 1)
        grupos_raw = _listar_grupos_cache(nome_competicao)
        equipes = _listar_equipes_competicao_cache(nome_competicao)
        grupos = _grupos_com_equipes_cacheados(nome_competicao, grupos_raw)
        config_agenda = _config_agenda_cache(nome_competicao)
        pacote_contexto.update({
            "grupo_unico_auto": _estrutura_grupo_unico(competicao),
            "quadra_unica_auto": len([q for q in quadras if q.get("ativa") is not False]) == 1,
            "grupos": grupos,
            "equipes": equipes,
            "quadras": quadras,
            "config_agenda": config_agenda,
            "config_geracao": config_agenda,
        })

    # Aba Partidas: carrega partidas, quadras e avanço; não calcula classificação.
    elif aba == "partidas":
        avanco = _avanco_cache(nome_competicao)
        status_avanco = _status_avanco_cache(nome_competicao)
        avanco_gerado = _avanco_gerado_cache(nome_competicao)
        status_avanco["gerado"] = avanco_gerado
        # GET da tabela nunca deve apagar partidas. Limpeza só em POST de geração.

        avanco_fases_tabs = _fases_do_avanco_para_tabela(avanco)
        series_fase = _series_do_avanco_por_fase(avanco, fase_subaba) if fase_subaba != "classificatorias" else []
        serie_ativa = (request.args.get("serie") or "").strip().lower()
        if series_fase and not any(s.get("id") == serie_ativa for s in series_fase):
            serie_ativa = series_fase[0].get("id")

        quadras = _quadras_cache(nome_competicao, competicao.get("qtd_quadras") or 1)
        grupos_raw = _listar_grupos_cache(nome_competicao)
        equipes = _listar_equipes_competicao_cache(nome_competicao)
        mapa_escudos = _mapa_escudos_equipes(equipes)
        # Partidas sempre frescas nesta aba para não travar placar ao vivo em cache.
        partidas = listar_partidas(nome_competicao) or []
        if not avanco_gerado:
            partidas = [p for p in partidas if not _partida_eh_avanco(p)]

        grupos = _grupos_com_equipes_cacheados(nome_competicao, grupos_raw)
        partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
        partidas_fase = _filtrar_partidas_por_fase(partidas_preparadas, fase_subaba)
        if fase_subaba != "classificatorias":
            partidas_fase = _filtrar_partidas_por_serie_avanco(partidas_fase, serie_ativa) if avanco_gerado else []
        avanco_espelho = _montar_espelho_avanco(avanco, partidas_preparadas, avanco_gerado)
        config_agenda = _config_agenda_cache(nome_competicao)
        pacote_contexto.update({
            "grupo_unico_auto": _estrutura_grupo_unico(competicao),
            "quadra_unica_auto": len([q for q in quadras if q.get("ativa") is not False]) == 1,
            "grupos": grupos,
            "equipes": equipes,
            "quadras": quadras,
            "partidas": partidas_preparadas,
            "partidas_fase": partidas_fase,
            "avanco": avanco,
            "avanco_status": status_avanco,
            "avanco_fases_tabs": avanco_fases_tabs,
            "avanco_series_fase": series_fase,
            "avanco_serie_ativa": serie_ativa,
            "avanco_espelho": avanco_espelho,
            "config_agenda": config_agenda,
            "config_geracao": config_agenda,
        })

    # Aba Classificação: carrega somente o necessário para calcular classificação.
    elif aba == "classificacao":
        grupos_raw = _listar_grupos_cache(nome_competicao)
        equipes = _listar_equipes_competicao_cache(nome_competicao)
        mapa_escudos = _mapa_escudos_equipes(equipes)
        partidas = _listar_partidas_cache(nome_competicao)
        partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
        grupos = _grupos_com_equipes_cacheados(nome_competicao, grupos_raw)
        classificacao, classificacao_do_cache = _calcular_ou_obter_classificacao_cacheada(nome_competicao, partidas_preparadas, grupos, competicao, mapa_escudos)
        regras_classificacao = _obter_regras_classificacao(competicao)
        criterios_classificacao = _criterios_efetivos_ate_sorteio(regras_classificacao.get("criterios"))
        colunas_classificacao = _colunas_classificacao_por_criterios(criterios_classificacao)
        pacote_contexto.update({
            "grupos": grupos,
            "equipes": equipes,
            "partidas": partidas_preparadas,
            "classificacao": classificacao,
            "criterios_classificacao": criterios_classificacao,
            "colunas_classificacao": colunas_classificacao,
        })

    # Aba Visualizador: gera/carrega o código curto e entrega o caminho pronto
    # ao template. Sem isso, o campo exibia somente o domínio porque
    # link_publico_path chegava vazio.
    elif aba == "visualizador":
        codigo_publico = garantir_codigo_publico_competicao(nome_competicao)

        if codigo_publico:
            link_publico_path = url_for(
                "tabela.visualizador_publico_curto",
                codigo_publico=codigo_publico,
            )
        else:
            # Fallback seguro: mantém o visualizador acessível mesmo se o banco
            # não conseguir criar o código curto naquele momento.
            link_publico_path = url_for(
                "tabela.visualizador_publico",
                competicao_nome=nome_competicao,
            )

        pacote_contexto.update({
            "codigo_publico": codigo_publico or "",
            "link_publico_path": link_publico_path,
            "link_publico": request.host_url.rstrip("/") + link_publico_path,
        })

    if pacote_contexto:
        if aba != "partidas":
            _pacote_cache_set(nome_competicao, aba, fase_subaba, serie_param_cache, pacote_contexto)
        contexto.update(pacote_contexto)

    html = _render_tabela_resposta(contexto)
    if aba == "geracao":
        html = _injetar_acoes_grupos_estrutura_html(html, competicao)
    return html


@tabela_bp.route("/tabela/api/resumo")
@exigir_organizador_da_competicao
def tabela_api_resumo():
    """API leve para futuras cargas por aba sem renderizar a tela inteira."""
    usuario = session.get("usuario")
    competicao = _buscar_competicao_organizador_cache(usuario)
    if not competicao:
        return jsonify({"ok": False, "erro": "competicao_nao_encontrada"}), 404
    nome = competicao["nome"]
    aba = (request.args.get("aba") or "geracao").strip().lower()
    return jsonify({
        "ok": True,
        "competicao": nome,
        "aba": aba,
        "grupos_travados": _grupos_travados_cache(nome),
        "competicao_travada": _competicao_travada_cache(nome),
    })


@tabela_bp.route("/tabela/avanco/gerar", methods=["POST"])
@exigir_organizador_da_competicao
def gerar_avanco_tabela_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = gerar_partidas_avanco_competicao(competicao["nome"])
    _limpar_cache_tabela(competicao["nome"])
    fase_destino = (request.form.get("fase_subaba") or "quartas").strip().lower()
    serie_destino = (request.form.get("serie") or "").strip().lower()

    if resultado.get("bloqueada"):
        pendentes = resultado.get("pendentes_classificatoria", 0)
        flash(f"Avanço bloqueado: ainda existem {pendentes} jogo(s) classificatório(s) pendente(s). Finalize todos antes de gerar os confrontos reais.", "erro")
    else:
        flash(
            f"Avanço gerado: {resultado.get('criadas', 0)} nova(s), {resultado.get('atualizadas', 0)} atualizada(s) e {resultado.get('duplicadas_removidas', 0)} duplicada(s) removida(s).",
            "sucesso",
        )

    args = {"aba": "partidas", "fase": fase_destino}
    if serie_destino:
        args["serie"] = serie_destino
    return redirect(url_for("tabela.tabela_view", **args))


# =========================================================
# CRIAR GRUPO
# =========================================================
@tabela_bp.route("/tabela/garantir-grupos", methods=["POST"])
@exigir_organizador_da_competicao
def garantir_grupos_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    _garantir_grupos_da_estrutura(competicao)
    if _estrutura_grupo_unico(competicao):
        flash("Grupo único sincronizado: todas as equipes ficam no Grupo A.", "sucesso")
    else:
        flash(f"Grupos criados conforme a competição: {', '.join(_nomes_grupos_automaticos(_qtd_grupos_configurada(competicao)))}. Agora preencha manualmente ou use o sorteio.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/sortear-grupos", methods=["POST"])
@exigir_organizador_da_competicao
def sortear_grupos_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _sortear_equipes_nos_grupos(competicao)
    flash(resultado.get("mensagem") or "Sorteio concluído.", "sucesso" if resultado.get("ok") else "erro")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/criar-grupo", methods=["POST"])
@exigir_organizador_da_competicao
def criar_grupo_view():
    nome = request.form.get("nome", "").strip().upper()
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível criar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    # Regra firme: os grupos vêm da Estrutura da competição.
    # Se for grupo único, usa Grupo A. Se forem 2+ grupos, usa A/B/C/D...
    # Assim ninguém cria grupo solto e a competição não volta a virar grupo único por engano.
    if _estrutura_grupo_unico(competicao) or _qtd_grupos_configurada(competicao) >= 1:
        _garantir_grupos_da_estrutura(competicao)
        flash("Os grupos são controlados pela Estrutura da competição. Eles foram sincronizados automaticamente.", "sucesso")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if not nome:
        flash("Informe o nome do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    criar_grupo(nome, competicao["nome"])

    flash("Grupo criado com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# VINCULAR GRUPO À QUADRA
# =========================================================
@tabela_bp.route("/tabela/grupo-quadra", methods=["POST"])
@exigir_organizador_da_competicao
def vincular_grupo_quadra_view():
    grupo_nome = (request.form.get("grupo_nome") or "").strip().upper()
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_nome:
        flash("Grupo inválido.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível trocar a quadra padrão do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if not quadra_id:
        flash("Selecione uma quadra válida.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if vincular_grupo_a_quadra(competicao["nome"], grupo_nome, quadra_id):
        flash(f"Grupo {grupo_nome} vinculado à quadra.", "sucesso")
    else:
        flash("Não foi possível vincular a quadra ao grupo.", "erro")

    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# ADICIONAR EQUIPE AO GRUPO
# =========================================================
@tabela_bp.route("/tabela/adicionar-equipe", methods=["POST"])
@exigir_organizador_da_competicao
def adicionar_equipe_grupo():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    adicionar_equipe_no_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe adicionada ao grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# REMOVER EQUIPE DO GRUPO
# =========================================================
@tabela_bp.route("/tabela/remover-equipe-grupo", methods=["POST"])
@exigir_organizador_da_competicao
def remover_equipe_grupo_view():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Dados inválidos para remover equipe do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    remover_equipe_do_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe removida do grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# EXCLUIR GRUPO
# =========================================================
@tabela_bp.route("/tabela/excluir-grupo/<int:grupo_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_grupo_view(grupo_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível excluir grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    excluir_grupo_banco(grupo_id, competicao["nome"])

    flash("Grupo excluído com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# LIMPEZA DE PARTIDAS
# =========================================================
@tabela_bp.route("/tabela/limpar", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_tabela():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível limpar toda a tabela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    ok = limpar_partidas(competicao["nome"])

    if ok is False:
        flash("Não foi possível limpar a tabela porque já existe partida iniciada.", "erro")
    else:
        flash("Tabela limpa com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/limpar-fase", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_fase_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível limpar as partidas dela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    ok = limpar_partidas_por_fase(competicao["nome"], fase_banco)

    if ok is False:
        flash("Não foi possível limpar esta fase porque já existe partida iniciada.", "erro")
    else:
        flash("Partidas da fase removidas com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# CRIAR PARTIDA MANUAL
# =========================================================
@tabela_bp.route("/tabela/nova-partida", methods=["POST"])
@exigir_organizador_da_competicao
def nova_partida():
    grupo = request.form.get("grupo")
    # Aceita os nomes principais e também alternativas, para não falhar se o template antigo ficar em cache.
    equipe_a = (request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante") or "").strip()
    equipe_b = (request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante") or "").strip()
    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))

    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_banco = _fase_subaba_para_banco(fase_subaba)

    # O mata-mata NÃO usa grupo. Grupo só é obrigatório nas classificatórias.
    if fase_banco == "grupos" and _estrutura_grupo_unico(competicao):
        _sincronizar_grupo_unico_automatico(competicao)
        grupo = "A"
    else:
        grupo = (grupo or "").strip().upper() if fase_banco == "grupos" else None
    if fase_banco == "grupos" and not quadra_id:
        quadra_id = _quadra_padrao_do_grupo(listar_grupos(competicao["nome"]), grupo)

    if fase_banco == "grupos" and not grupo:
        flash("Informe o grupo para jogo classificatório.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    # Regra principal:
    # - grupos travam quando a classificatória inicia;
    # - mata-mata só trava quando a própria fase iniciar.
    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível criar novas partidas nela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    if fase_banco == "grupos":
        if not equipe_a or not equipe_b:
            flash("Selecione as duas equipes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

        if equipe_a == equipe_b:
            flash("A partida precisa ter duas equipes diferentes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))
    else:
        # Mata-mata manual pode ser criado antes do fim da classificatória.
        # Se o organizador ainda não quiser escolher as equipes, salva como A definir.
        if equipe_a and equipe_b and equipe_a == equipe_b:
            flash("A partida precisa ter duas equipes diferentes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

        equipe_a = equipe_a or "A definir"
        equipe_b = equipe_b or "A definir"

    partidas = listar_partidas(competicao["nome"])
    ordens = []
    for partida in partidas:
        try:
            ordens.append(int(partida.get("ordem") or 0))
        except (TypeError, ValueError):
            pass
    ordem = (max(ordens) + 1) if ordens else 1

    ok_criacao = _criar_partida_para_tabela(
        competicao["nome"],
        grupo,
        equipe_a,
        equipe_b,
        ordem,
        fase_banco,
        origem="manual",
        quadra_id=quadra_id,
    )

    if not ok_criacao:
        flash("Não foi possível criar a partida. Verifique se esta fase já iniciou.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    flash("Partida criada com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))



# =========================================================
# ATUALIZAR PARTIDA MANUAL DO MATA-MATA
# =========================================================
@tabela_bp.route("/tabela/atualizar-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def atualizar_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))
    data_hora = (request.form.get("data_hora") or "").strip() or None
    rodada = _to_int_or_none(request.form.get("rodada"))

    partida_atual = buscar_partida_por_id(partida_id, competicao["nome"]) or {}
    equipe_a = (request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante") or partida_atual.get("equipe_a") or "").strip()
    equipe_b = (request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante") or partida_atual.get("equipe_b") or "").strip()
    if rodada is None:
        rodada = _to_int_or_none(partida_atual.get("rodada"))

    if equipe_a and equipe_b and equipe_a == equipe_b:
        flash("A partida precisa ter duas equipes diferentes.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    equipe_a = equipe_a or "A definir"
    equipe_b = equipe_b or "A definir"

    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível alterar partidas dela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    quadra_id, quadra_nome = _dados_quadra(competicao["nome"], quadra_id)

    ok = atualizar_partida(
        partida_id,
        competicao["nome"],
        partida_atual.get("grupo"),
        fase_banco,
        equipe_a,
        equipe_b,
        quadra=str(quadra_id) if quadra_id else None,
        quadra_id=quadra_id,
        quadra_nome=quadra_nome,
        data_hora=data_hora,
        rodada=rodada,
        status="aguardando",
    )

    if ok is False:
        flash("Não foi possível salvar. A partida já iniciou ou está bloqueada.", "erro")
    else:
        flash("Partida salva com sucesso.", "sucesso")

    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# EXCLUIR PARTIDA
# =========================================================
@tabela_bp.route("/tabela/excluir-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()

    ok, mensagem = excluir_partida_banco(partida_id, competicao["nome"])
    flash(mensagem, "sucesso" if ok else "erro")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))



# =========================================================
# MOTOR INTELIGENTE DE AGENDA DA FASE CLASSIFICATÓRIA
# =========================================================
def _gerar_rodadas_round_robin(equipes):
    """Gera rodadas reais todos-contra-todos pelo método do círculo.

    Rodada aqui NÃO é a ordem física da partida. Rodada é o bloco lógico em que
    uma equipe joga no máximo uma vez. Exemplo com 6 equipes: 5 rodadas, cada
    rodada com 3 jogos. Com número ímpar, uma equipe folga e a folga gira.
    """
    times = list(equipes or [])
    if len(times) < 2:
        return []

    if len(times) % 2 == 1:
        times.append(None)

    n = len(times)
    rodadas = []

    for rodada_idx in range(n - 1):
        jogos = []
        folga = None
        for i in range(n // 2):
            t1 = times[i]
            t2 = times[n - 1 - i]
            if t1 is None or t2 is None:
                folga = t1 or t2
                continue

            # Alterna mando/ordem visual para não deixar sempre o mesmo time primeiro.
            if rodada_idx % 2 == 0:
                jogos.append((t1, t2))
            else:
                jogos.append((t2, t1))

        rodadas.append({
            "numero": rodada_idx + 1,
            "jogos": jogos,
            "folga": folga,
        })
        times = [times[0]] + [times[-1]] + times[1:-1]

    return rodadas


def _numero_rodada_info(rodada_info, padrao=1):
    if isinstance(rodada_info, dict):
        try:
            return int(rodada_info.get("numero") or padrao)
        except (TypeError, ValueError):
            return padrao
    return padrao


def _jogos_rodada_info(rodada_info):
    """Retorna somente confrontos válidos (equipe_a, equipe_b).

    Compatibilidade importante:
    - _gerar_rodadas_round_robin() retorna dict com {"numero", "jogos", "folga"};
    - versões antigas/rotas auxiliares podem mandar lista de tuplas;
    - nunca devemos transformar dict em list(dict), porque isso vira
      ["numero", "jogos", "folga"] e causa ValueError no unpack.
    """
    if isinstance(rodada_info, dict):
        jogos_raw = rodada_info.get("jogos") or []
    else:
        jogos_raw = rodada_info or []

    jogos = []
    for jogo in jogos_raw:
        if isinstance(jogo, dict):
            equipe_a = jogo.get("equipe_a") or jogo.get("a") or jogo.get("time_a")
            equipe_b = jogo.get("equipe_b") or jogo.get("b") or jogo.get("time_b")
        elif isinstance(jogo, (list, tuple)) and len(jogo) >= 2:
            equipe_a, equipe_b = jogo[0], jogo[1]
        else:
            continue

        if equipe_a and equipe_b:
            jogos.append((equipe_a, equipe_b))

    return jogos


def _ids_quadras_ativas(quadras):
    ids = []
    for q in quadras or []:
        if q.get("ativa") is False:
            continue
        try:
            ids.append(int(q.get("id")))
        except (TypeError, ValueError):
            pass
    return ids


def _normalizar_lista_ids(valores):
    if valores in (None, ""):
        return []
    if isinstance(valores, str):
        try:
            valores = json.loads(valores)
        except Exception:
            valores = [v.strip() for v in valores.split(",")]
    ids = []
    for v in valores or []:
        try:
            n = int(v)
            if n > 0 and n not in ids:
                ids.append(n)
        except (TypeError, ValueError):
            pass
    return ids


def _parse_grupos_compartilhados_form():
    """Lê configurações opcionais do formulário sem depender do HTML novo.

    Aceita formatos simples:
    - grupos_compartilhados_json = {"A":[1,2], "B":[1,2]}
    - quadras_compartilhadas_json = [1,2]
    - grupo_quadras_A = 1,2
    """
    bruto = request.form.get("grupos_compartilhados_json") or request.form.get("grupos_compartilhados")
    if bruto:
        try:
            dados = json.loads(bruto)
            if isinstance(dados, dict):
                return {str(k).strip().upper(): _normalizar_lista_ids(v) for k, v in dados.items()}
        except Exception:
            pass

    dados = {}
    for chave, valor in request.form.items():
        if not chave.startswith("grupo_quadras_"):
            continue
        grupo = chave.replace("grupo_quadras_", "", 1).strip().upper()
        ids = _normalizar_lista_ids(valor)
        if grupo and ids:
            dados[grupo] = ids
    return dados


def _config_agenda_da_requisicao(nome_competicao):
    inicializar_configuracao_agenda_competicao(nome_competicao)
    config = buscar_configuracao_agenda_competicao(nome_competicao) or {}

    if request.method == "POST":
        modo = (request.form.get("modo_distribuicao") or request.form.get("modo_distribuicao_agenda") or config.get("modo_distribuicao") or "automatico_inteligente").strip().lower()
        rodizio = (request.form.get("rodizio_grupos") or config.get("rodizio_grupos") or "por_rodada").strip().lower()
        descanso = request.form.get("descanso_minimo_jogos", config.get("descanso_minimo_jogos", 1))
        permitir_relaxar = request.form.get("permitir_relaxar_descanso")
        if permitir_relaxar is None:
            permitir_relaxar = config.get("permitir_relaxar_descanso", True)
        else:
            permitir_relaxar = str(permitir_relaxar).strip().lower() in {"1", "true", "on", "sim", "yes"}

        grupos_comp = _parse_grupos_compartilhados_form() or config.get("grupos_compartilhados") or {}
        quadras_comp = _normalizar_lista_ids(
            request.form.get("quadras_compartilhadas_json")
            or request.form.get("quadras_compartilhadas")
            or config.get("quadras_compartilhadas")
        )

        atualizar_configuracao_agenda_competicao(
            nome_competicao,
            modo_distribuicao=modo,
            descanso_minimo_jogos=descanso,
            rodizio_grupos=rodizio,
            permitir_relaxar_descanso=permitir_relaxar,
            grupos_compartilhados=grupos_comp,
            quadras_compartilhadas=quadras_comp,
            usar_rodadas_programadas=config.get("usar_rodadas_programadas", False),
            uma_partida_por_equipe_rodada=config.get("uma_partida_por_equipe_rodada", True),
        )
        config = buscar_configuracao_agenda_competicao(nome_competicao) or config

    return config


def _quadras_permitidas_para_grupo(nome_competicao, grupos_raw, grupo_nome, quadras_ativas, config):
    """Define quais quadras o grupo pode usar.

    REGRA IMPORTANTE:
    - Se o grupo tem quadra padrão definida na aba Configurações, ele fica FIXO nessa quadra.
    - Uma quadra fixa de um grupo fica reservada para esse grupo.
    - Grupos sem quadra definida usam somente as quadras livres, ou seja, não invadem
      quadras reservadas por grupos fixos.
    - Só usamos uma configuração específica do modal quando ela existir para o grupo.
    """
    grupo_nome = str(grupo_nome or "").strip().upper()
    quadras_ativas = [qid for qid in (quadras_ativas or []) if qid is not None]
    if not quadras_ativas:
        return []

    # Mapa de quadras fixas cadastradas na aba Configurações.
    fixas_por_grupo = {}
    quadras_reservadas = set()
    for g in grupos_raw or []:
        nome_g = str(g.get("nome") or "").strip().upper()
        qid = _quadra_id_do_grupo(g)
        try:
            qid = int(qid or 0)
        except (TypeError, ValueError):
            qid = None
        if nome_g and qid and qid in quadras_ativas:
            fixas_por_grupo[nome_g] = qid
            quadras_reservadas.add(qid)

    # 1) Grupo com quadra definida é sempre fixo.
    if grupo_nome in fixas_por_grupo:
        return [fixas_por_grupo[grupo_nome]]

    # 2) Para grupos sem quadra definida, remove as quadras reservadas para grupos fixos.
    quadras_livres = [qid for qid in quadras_ativas if qid not in quadras_reservadas]
    if not quadras_livres:
        # Se todas as quadras estão reservadas, libera fallback para não travar a geração.
        quadras_livres = list(quadras_ativas)

    compartilhados = (config or {}).get("grupos_compartilhados") or {}
    quadras_compartilhadas = _normalizar_lista_ids((config or {}).get("quadras_compartilhadas"))

    # 3) Configuração específica por grupo no modal, filtrada pelas quadras livres.
    ids = _normalizar_lista_ids(compartilhados.get(grupo_nome) or compartilhados.get(grupo_nome.lower()))
    ids = [qid for qid in ids if qid in quadras_livres]
    if ids:
        return ids

    # 4) Pool geral compartilhado, também sem invadir quadras reservadas.
    if quadras_compartilhadas:
        ids = [qid for qid in quadras_compartilhadas if qid in quadras_livres]
        if ids:
            return ids

    # 5) Fallback: qualquer quadra livre.
    return list(quadras_livres)


def _montar_fila_jogos_classificatorios(rodadas_por_grupo, rodizio):
    """Monta uma fila respeitando rodadas reais entre grupos."""
    fila = []
    grupos = sorted(rodadas_por_grupo.keys())
    max_rodadas = max((len(r) for r in rodadas_por_grupo.values()), default=0)

    if rodizio == "por_grupo_inteiro":
        for grupo in grupos:
            for pos, rodada_info in enumerate(rodadas_por_grupo.get(grupo) or [], start=1):
                rodada_num = _numero_rodada_info(rodada_info, pos)
                for equipe_a, equipe_b in _jogos_rodada_info(rodada_info):
                    fila.append({"grupo": grupo, "rodada_grupo": rodada_num, "equipe_a": equipe_a, "equipe_b": equipe_b})
        return fila

    # Padrão: primeiro todas as Rodadas 1 dos grupos, depois Rodadas 2, etc.
    for rodada_idx in range(max_rodadas):
        for grupo in grupos:
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            rodada_info = rodadas[rodada_idx]
            rodada_num = _numero_rodada_info(rodada_info, rodada_idx + 1)
            for equipe_a, equipe_b in _jogos_rodada_info(rodada_info):
                fila.append({"grupo": grupo, "rodada_grupo": rodada_num, "equipe_a": equipe_a, "equipe_b": equipe_b})
    return fila


def _jogo_respeita_descanso(jogo, historico_slots, descanso_minimo):
    if descanso_minimo <= 0:
        return True
    equipes = {jogo["equipe_a"], jogo["equipe_b"]}
    for slot in historico_slots[-descanso_minimo:]:
        if equipes.intersection(slot):
            return False
    return True


def _proximo_jogo_sem_conflito(lista_jogos, equipes_slot, equipes_slot_anterior=None):
    """Remove e retorna o primeiro jogo possível sem conflito no slot.

    Primeiro tenta evitar equipes que jogaram no slot anterior. Se não existir
    opção, relaxa essa regra para não travar grupos com poucos times/quadra única
    como o caso da Apolo.
    """
    equipes_slot = set(equipes_slot or set())
    equipes_slot_anterior = set(equipes_slot_anterior or set())

    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get("equipe_a"), jogo.get("equipe_b")}
        if equipes.intersection(equipes_slot):
            continue
        if equipes_slot_anterior and equipes.intersection(equipes_slot_anterior):
            continue
        return lista_jogos.pop(idx)

    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get("equipe_a"), jogo.get("equipe_b")}
        if equipes.intersection(equipes_slot):
            continue
        return lista_jogos.pop(idx)

    return None


def _montar_blocos_por_pool_classificatoria(nome_competicao, grupos_raw, quadras_ativas, config):
    """Agrupa os grupos pelo conjunto de quadras que eles podem usar.

    Exemplo prático:
    - Grupo C permite apenas Apolo => pool (Apolo)
    - Grupos A/B/D permitem Floresta 1 e 2 => pool (Floresta 1, Floresta 2)

    Isso é o que permite gerar rodadas simultâneas por local/quadras sem misturar
    um grupo fixo com grupos rotativos.
    """
    pools = {}
    for g in grupos_raw or []:
        grupo = str(g.get("nome") or "").strip().upper()
        if not grupo:
            continue
        permitidas = _quadras_permitidas_para_grupo(nome_competicao, grupos_raw, grupo, quadras_ativas, config)
        permitidas = tuple(qid for qid in permitidas if qid in quadras_ativas)
        if not permitidas:
            continue
        pools.setdefault(permitidas, []).append(grupo)

    # Pools com mais quadras primeiro. Na prática, Floresta vem antes da Apolo,
    # mas o slot final continua sincronizado por número de linha.
    return dict(sorted(pools.items(), key=lambda item: (-len(item[0]), item[0])))


def _grupo_com_mais_rodadas_restantes(rodadas_por_grupo, grupos_pool, ultimo_grupo=None):
    candidatos = []
    for grupo in grupos_pool or []:
        restante = len(rodadas_por_grupo.get(grupo) or [])
        if restante <= 0:
            continue
        if ultimo_grupo and grupo == ultimo_grupo and len(grupos_pool) > 1:
            continue
        candidatos.append((restante, grupo))

    if not candidatos and ultimo_grupo:
        for grupo in grupos_pool or []:
            restante = len(rodadas_por_grupo.get(grupo) or [])
            if restante > 0:
                candidatos.append((restante, grupo))

    if not candidatos:
        return None

    # Maior quantidade restante ganha. Em empate, ordem alfabética/visual.
    candidatos.sort(key=lambda x: (-x[0], x[1]))
    return candidatos[0][1]


def _gerar_slots_pool_multiquadra(rodadas_por_grupo, grupos_pool, quadras_pool):
    """Gera slots físicos mantendo a rodada lógica correta.

    A quadra/slot serve só para ordenar e distribuir jogos. O campo `rodada_grupo`
    continua sendo a rodada real do todos-contra-todos. Assim, se o Grupo A tem
    6 equipes, a Rodada 1 fica com 3 jogos, mesmo que precise de mais de um slot
    físico para executar todos eles.
    """
    capacidade = max(1, len(quadras_pool or []))
    slots = []
    max_rodadas = max((len(rodadas_por_grupo.get(g) or []) for g in grupos_pool or []), default=0)

    for rodada_idx in range(max_rodadas):
        for grupo in sorted(grupos_pool or []):
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue

            rodada_info = rodadas[rodada_idx]
            rodada_num = _numero_rodada_info(rodada_info, rodada_idx + 1)
            jogos = _jogos_rodada_info(rodada_info)

            while jogos:
                jogos_slot = []
                equipes_slot = set()
                for qid in quadras_pool[:capacidade]:
                    if not jogos:
                        break
                    equipe_a, equipe_b = jogos.pop(0)
                    if equipe_a in equipes_slot or equipe_b in equipes_slot:
                        jogos.insert(0, (equipe_a, equipe_b))
                        break
                    jogos_slot.append({
                        "grupo": grupo,
                        "equipe_a": equipe_a,
                        "equipe_b": equipe_b,
                        "quadra_id": qid,
                        "rodada_grupo": rodada_num,
                    })
                    equipes_slot.update({equipe_a, equipe_b})
                if jogos_slot:
                    slots.append(jogos_slot)
                else:
                    break

    return slots


def _gerar_slots_pool_quadra_unica(rodadas_por_grupo, grupos_pool, quadra_id):
    """Gera slots para uma quadra só sem transformar cada jogo em nova rodada.

    Com uma quadra, os jogos são sequenciais, mas a rodada lógica permanece: a
    Rodada 1 mostra todos os jogos da Rodada 1, depois a Rodada 2, e assim vai.
    """
    slots = []
    max_rodadas = max((len(rodadas_por_grupo.get(g) or []) for g in grupos_pool or []), default=0)

    for rodada_idx in range(max_rodadas):
        for grupo in sorted(grupos_pool or []):
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            rodada_info = rodadas[rodada_idx]
            rodada_num = _numero_rodada_info(rodada_info, rodada_idx + 1)
            for equipe_a, equipe_b in _jogos_rodada_info(rodada_info):
                slots.append([{
                    "grupo": grupo,
                    "equipe_a": equipe_a,
                    "equipe_b": equipe_b,
                    "quadra_id": quadra_id,
                    "rodada_grupo": rodada_num,
                }])

    return slots


def _gerar_agenda_classificatoria_inteligente(nome_competicao, grupos_raw, config):
    """Gera a classificatória por SLOTS simultâneos.

    A lógica principal agora é:
    1. gerar os confrontos de cada grupo em memória;
    2. separar grupos por pool de quadras permitidas;
    3. em pools com 2+ quadras, colocar um bloco/rodada do mesmo grupo por slot;
    4. alternar grupos pelo maior número de rodadas restantes, evitando grupo repetido;
    5. salvar o slot em `rodada`, para a tela entender que Floresta 1 e 2 acontecem juntas.
    """
    quadras = garantir_quadras_competicao(nome_competicao, 1)
    quadras_ativas = _ids_quadras_ativas(quadras)
    if not quadras_ativas:
        quadras_ativas = [None]

    rodadas_por_grupo = {}
    for g in grupos_raw or []:
        equipes = listar_equipes_por_grupo(g["id"])
        nomes = [e.get("equipe") for e in equipes if e.get("equipe")]
        if len(nomes) >= 2:
            rodadas_por_grupo[str(g.get("nome") or "").strip().upper()] = _gerar_rodadas_round_robin(nomes)

    if not rodadas_por_grupo:
        return {"ok": False, "mensagem": "Não há grupos com equipes suficientes para gerar jogos."}

    pools = _montar_blocos_por_pool_classificatoria(nome_competicao, grupos_raw, quadras_ativas, config)
    if not pools:
        return {"ok": False, "mensagem": "Não foi possível definir as quadras permitidas dos grupos."}

    slots_por_pool = []
    for quadras_pool, grupos_pool in pools.items():
        # Copia só as rodadas dos grupos deste pool para não consumir o dict global.
        rodadas_pool = {
            # Mantém o dict da rodada intacto.
            # Antes usava list(rodada), que em dict vira ["numero", "jogos", "folga"]
            # e quebrava a geração automática em quadra única.
            g: [
                dict(rodada) if isinstance(rodada, dict) else list(rodada)
                for rodada in (rodadas_por_grupo.get(g) or [])
            ]
            for g in grupos_pool
        }

        if len(quadras_pool) >= 2:
            slots_pool = _gerar_slots_pool_multiquadra(rodadas_pool, grupos_pool, list(quadras_pool))
        else:
            slots_pool = _gerar_slots_pool_quadra_unica(rodadas_pool, grupos_pool, quadras_pool[0])

        slots_por_pool.append(slots_pool)

    total_slots = max((len(s) for s in slots_por_pool), default=0)
    agenda = []

    for slot_idx in range(total_slots):
        slot_numero = slot_idx + 1
        ordem_no_slot = 1
        for slots_pool in slots_por_pool:
            if slot_idx >= len(slots_pool):
                continue
            for jogo in slots_pool[slot_idx]:
                item = dict(jogo)
                item["slot"] = slot_numero
                item["ordem_no_slot"] = ordem_no_slot
                item["rodada_grupo"] = item.get("rodada_grupo") or slot_numero
                agenda.append(item)
                ordem_no_slot += 1

    if not agenda:
        return {"ok": False, "mensagem": "Não foi possível montar a agenda dos jogos."}

    return {"ok": True, "agenda": agenda, "slots": total_slots, "quadras": len(quadras_ativas)}



# =========================================================
# SALVAR CONFIGURAÇÃO DA GERAÇÃO AUTOMÁTICA
# =========================================================
@tabela_bp.route("/tabela/salvar-config-geracao", methods=["POST"])
@exigir_organizador_da_competicao
def salvar_config_geracao_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar a configuração da geração.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    # Compatibilidade com o modal do tabela.html atual.
    modo_form = (request.form.get("modo_distribuicao") or "rodizio").strip().lower()
    if modo_form in {"fixa", "grupo_fixo", "fixo"}:
        modo = "grupo_fixo"
    else:
        modo = "automatico_inteligente"

    ordem_form = (request.form.get("ordem_jogos") or "intercalar_grupos").strip().lower()
    mapa_ordem = {
        "intercalar_grupos": "por_rodada",
        "por_grupo": "por_grupo_inteiro",
        "balancear_quadras": "por_rodada",
    }
    rodizio = mapa_ordem.get(ordem_form, "por_rodada")

    try:
        descanso = int(request.form.get("descanso_minimo") or 1)
    except (TypeError, ValueError):
        descanso = 1
    descanso = max(0, min(descanso, 5))

    grupos_raw = listar_grupos(competicao["nome"])
    grupos_compartilhados = {}
    for g in grupos_raw:
        nome_g = str(g.get("nome") or "").strip().upper()
        if not nome_g:
            continue

        # O HTML envia quadras_grupo_A[]; o request.form.getlist aceita esse nome completo.
        ids = []
        for valor in request.form.getlist(f"quadras_grupo_{nome_g}[]"):
            try:
                qid = int(valor)
                if qid > 0 and qid not in ids:
                    ids.append(qid)
            except (TypeError, ValueError):
                pass
        if ids:
            grupos_compartilhados[nome_g] = ids

    quadras_compartilhadas = []
    for ids in grupos_compartilhados.values():
        for qid in ids:
            if qid not in quadras_compartilhadas:
                quadras_compartilhadas.append(qid)

    atualizar_configuracao_agenda_competicao(
        competicao["nome"],
        modo_distribuicao=modo,
        descanso_minimo_jogos=descanso,
        rodizio_grupos=rodizio,
        permitir_relaxar_descanso=True,
        grupos_compartilhados=grupos_compartilhados,
        quadras_compartilhadas=quadras_compartilhadas,
    )

    flash("Configuração da geração automática salva com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))


# =========================================================
# GERAR JOGOS AUTOMÁTICOS
# =========================================================
@tabela_bp.route("/tabela/gerar-automatico", methods=["POST"])
@exigir_organizador_da_competicao
def gerar_automatico():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    nome_competicao = competicao["nome"]
    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    # Esta validação é feita uma única vez. Antes, algumas funções de criação
    # podiam repetir consultas de trava para cada partida gerada.
    if not _fase_pode_ser_alterada_sem_travar_mata_mata(nome_competicao, fase_banco):
        flash("Esta fase já iniciou. Não é possível gerar jogos automaticamente nela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    _garantir_grupos_da_estrutura(competicao)
    grupos_raw = _listar_grupos_cache(nome_competicao)
    mapa_quadras = _mapa_quadras_formatadas(nome_competicao)

    if fase_banco != "grupos":
        serie_ativa = (request.form.get("serie") or request.args.get("serie") or "").strip().lower()
        partidas = _listar_partidas_cache(nome_competicao)
        equipes_competicao = _listar_equipes_competicao_cache(nome_competicao)
        mapa_escudos = _mapa_escudos_equipes(equipes_competicao)
        partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)

        def _filtrar_serie_atual(lista):
            if not serie_ativa:
                return list(lista or [])
            filtradas = []
            for p in lista or []:
                serie_p, _jogo_id = _origem_partida_avanco(p)
                # Partidas antigas sem origem de avanço continuam visíveis só
                # quando não há série selecionada. Isso impede Prata de mexer na Ouro.
                if serie_p == serie_ativa:
                    filtradas.append(p)
            return filtradas

        def _vencedor_ou_placeholder(partida, prefixo, indice):
            return _vencedor_partida_rapido(partida, f"Vencedor {prefixo} {indice}")

        confrontos = []

        # Só calcula classificação quando a fase realmente depende dela:
        # quartas sempre; semifinal apenas se não existirem quartas suficientes.
        classificacao = None
        def _obter_classificacao_para_mata_mata():
            nonlocal classificacao
            if classificacao is not None:
                return classificacao
            grupos = _grupos_com_equipes_cacheados(nome_competicao, grupos_raw, incluir_quadra=False)
            classificacao, _classificacao_do_cache = _calcular_ou_obter_classificacao_cacheada(
                nome_competicao, partidas_preparadas, grupos, competicao, mapa_escudos
            )
            return classificacao

        if fase_banco == "quartas":
            classificados = _ordenar_classificados_intercalado(_obter_classificacao_para_mata_mata())
            if len(classificados) < 8:
                flash("Para gerar quartas automaticamente, precisa ter pelo menos 8 equipes classificadas.", "erro")
                return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))

            top8 = classificados[:8]
            confrontos = [
                (top8[0], top8[7]),
                (top8[3], top8[4]),
                (top8[1], top8[6]),
                (top8[2], top8[5]),
            ]
        elif fase_banco == "semifinal":
            quartas = _filtrar_partidas_por_fase(partidas_preparadas, "quartas")
            quartas = _filtrar_serie_atual(quartas)
            quartas = sorted(quartas, key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))
            if len(quartas) >= 4:
                confrontos = [
                    (_vencedor_ou_placeholder(quartas[0], "Quartas", 1), _vencedor_ou_placeholder(quartas[1], "Quartas", 2)),
                    (_vencedor_ou_placeholder(quartas[2], "Quartas", 3), _vencedor_ou_placeholder(quartas[3], "Quartas", 4)),
                ]
            else:
                classificados = _ordenar_classificados_intercalado(_obter_classificacao_para_mata_mata())
                if len(classificados) < 4:
                    flash("Para gerar semifinais automaticamente, precisa ter quartas criadas ou pelo menos 4 equipes classificadas.", "erro")
                    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))
                top4 = classificados[:4]
                confrontos = [(top4[0], top4[3]), (top4[1], top4[2])]
        elif fase_banco == "final":
            semis = _filtrar_partidas_por_fase(partidas_preparadas, "semifinais")
            semis = _filtrar_serie_atual(semis)
            semis = sorted(semis, key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))
            if len(semis) < 2:
                flash("Para gerar a final automaticamente, crie as duas semifinais primeiro.", "erro")
                return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))
            confrontos = [(_vencedor_ou_placeholder(semis[0], "Semifinal", 1), _vencedor_ou_placeholder(semis[1], "Semifinal", 2))]
        elif fase_banco == "terceiro_lugar":
            semis = _filtrar_partidas_por_fase(partidas_preparadas, "semifinais")
            semis = _filtrar_serie_atual(semis)
            semis = sorted(semis, key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))
            if len(semis) < 2:
                flash("Para gerar 3º lugar automaticamente, crie as duas semifinais primeiro.", "erro")
                return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))
            def _perdedor(partida, prefixo, indice):
                vencedor = _vencedor_partida_rapido(partida, "")
                if vencedor and vencedor == partida.get("equipe_a"):
                    return partida.get("equipe_b") or f"Perdedor {prefixo} {indice}"
                if vencedor and vencedor == partida.get("equipe_b"):
                    return partida.get("equipe_a") or f"Perdedor {prefixo} {indice}"
                return f"Perdedor {prefixo} {indice}"
            confrontos = [(_perdedor(semis[0], "Semifinal", 1), _perdedor(semis[1], "Semifinal", 2))]

        if not confrontos:
            flash("Não foi possível montar confrontos automáticos para esta fase.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))

        removidas = _limpar_partidas_fase_serie_nao_iniciadas(nome_competicao, fase_banco, serie_ativa)
        ordem_inicial = max([int(p.get("ordem") or 0) for p in partidas if p.get("ordem") is not None] or [0]) + 1
        quadra_id, quadra_nome = _quadra_nome_cache(mapa_quadras, _to_int_or_none(request.form.get("quadra_id")))

        partidas_para_salvar = []
        for indice, (equipe_a, equipe_b) in enumerate(confrontos, start=1):
            origem = f"avanco:{serie_ativa}:auto_{fase_banco}_{indice}" if serie_ativa else "automatica"
            partidas_para_salvar.append({
                "competicao": nome_competicao,
                "grupo": None,
                "equipe_a": equipe_a,
                "equipe_b": equipe_b,
                "fase": fase_banco,
                "ordem": ordem_inicial + indice - 1,
                "quadra_id": quadra_id,
                "quadra_nome": quadra_nome,
                "origem": origem,
                "rodada": indice,
                "data_hora": buscar_data_hora_rodada_programada(nome_competicao, "avanco", fase_banco, serie_ativa, indice),
            })

        total_inserido = _inserir_partidas_em_lote(partidas_para_salvar)
        _limpar_cache_tabela_e_classificacao(nome_competicao)

        flash(f"Jogos do mata-mata gerados: {total_inserido} criado(s), {removidas} pendente(s) removido(s). Partidas com resultado não foram alteradas.", "sucesso")
        args = {"aba": "partidas", "fase": fase_subaba}
        if serie_ativa:
            args["serie"] = serie_ativa
        return redirect(url_for("tabela.tabela_view", **args))

    config_agenda = _config_agenda_da_requisicao(nome_competicao)
    resultado_agenda = _gerar_agenda_classificatoria_inteligente(nome_competicao, grupos_raw, config_agenda)

    if not resultado_agenda.get("ok"):
        flash(resultado_agenda.get("mensagem") or "Não foi possível gerar os jogos automaticamente.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    limpar_partidas_por_fase(nome_competicao, "grupos")

    partidas_para_salvar = []
    for ordem, jogo in enumerate(resultado_agenda.get("agenda") or [], start=1):
        quadra_id, quadra_nome = _quadra_nome_cache(mapa_quadras, jogo.get("quadra_id"))
        partidas_para_salvar.append({
            "competicao": nome_competicao,
            "grupo": jogo["grupo"],
            "equipe_a": jogo["equipe_a"],
            "equipe_b": jogo["equipe_b"],
            "fase": "grupos",
            "ordem": ordem,
            "quadra_id": quadra_id,
            "quadra_nome": quadra_nome,
            "origem": "automatica_inteligente",
            "rodada": jogo.get("rodada_grupo") or jogo.get("slot"),
            "data_hora": buscar_data_hora_rodada_programada(nome_competicao, "classificatoria", "grupos", "", jogo.get("rodada_grupo") or jogo.get("slot")),
        })

    total_inserido = _inserir_partidas_em_lote(partidas_para_salvar)
    _limpar_cache_tabela(nome_competicao)

    flash(
        f"{total_inserido} jogos gerados automaticamente com agenda inteligente: "
        f"{max([int(j.get('rodada_grupo') or 0) for j in (resultado_agenda.get('agenda') or [])] or [resultado_agenda.get('slots', 0)])} rodada(s) reais, "
        f"descanso mínimo de {config_agenda.get('descanso_minimo_jogos', 1)} jogo(s) quando possível.",
        "sucesso",
    )
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

