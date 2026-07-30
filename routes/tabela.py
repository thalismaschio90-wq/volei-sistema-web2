from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify
from functools import wraps
import json
import os
import time
import hashlib

from services.competicoes.tabela_gateway import (
    buscar_competicao_por_organizador,
    buscar_competicao_por_nome,
    criar_grupo,
    listar_grupos,
    listar_equipes_por_grupo,
    listar_equipes_por_grupos_competicao,
    criar_partida,
    listar_partidas,
    listar_partidas_leve,
    proxima_ordem_partida,
    buscar_partida_por_id,
    limpar_partidas_por_fase,
    atualizar_partida,
    competicao_esta_travada,
    fase_grupos_esta_travada_por_jogo,
    listar_quadras_competicao,
    garantir_quadras_competicao,
    buscar_quadra_competicao_por_id,
    formatar_quadra_exibicao,
    normalizar_vinculos_quadras_competicao,
    conectar,
    buscar_configuracao_agenda_competicao,
    atualizar_configuracao_agenda_competicao,
    _buscar_colunas_tabela,
    buscar_avanco_config_competicao,
    gerar_partidas_avanco_competicao,
    status_avanco_classificatorias_competicao,
    avanco_ja_gerado_competicao,
    buscar_data_hora_rodada_programada,
    garantir_codigo_publico_competicao,
    buscar_competicao_por_codigo_publico,
)
from services.equipes.consultas import listar_equipes_da_competicao
from services.competicoes.mata_mata import gerar_e_persistir_mata_mata
from services.competicoes.geracao_partidas import (
    gerar_agenda_classificatoria as _gerar_agenda_classificatoria_inteligente,
    inserir_partidas_em_lote as _inserir_partidas_em_lote_service,
)
from rules.agenda_partidas import (
    gerar_rodadas_round_robin as _gerar_rodadas_round_robin,
    numero_rodada_info as _numero_rodada_info,
    jogos_rodada_info as _jogos_rodada_info,
    ids_quadras_ativas as _ids_quadras_ativas,
    normalizar_lista_ids as _normalizar_lista_ids,
    montar_fila_jogos_classificatorios as _montar_fila_jogos_classificatorios,
    jogo_respeita_descanso as _jogo_respeita_descanso,
    proximo_jogo_sem_conflito as _proximo_jogo_sem_conflito,
    grupo_com_mais_rodadas_restantes as _grupo_com_mais_rodadas_restantes,
    gerar_slots_pool_multiquadra as _gerar_slots_pool_multiquadra,
    gerar_slots_pool_quadra_unica as _gerar_slots_pool_quadra_unica,
)


from services.competicoes.classificacao import (
    calcular_classificacao as _calcular_classificacao,
    calcular_ou_obter_classificacao_cacheada as _calcular_ou_obter_classificacao_cacheada,
    colunas_classificacao_publica as _colunas_classificacao_publica,
    colunas_classificacao_por_criterios as _colunas_classificacao_por_criterios,
    criterios_efetivos_ate_sorteio as _criterios_efetivos_ate_sorteio,
    competicao_eh_set_unico_tabela as _competicao_eh_set_unico_tabela,
    obter_regras_classificacao as _obter_regras_classificacao,
    to_bool as _to_bool,
)

from rules.visualizador_publico import (
    bool_publico as _bool_publico,
    modo_scout_ativo_publico as _modo_scout_ativo_publico,
    evento_detalhes_publico as _evento_detalhes_publico,
    lado_para_nome_publico as _lado_para_nome_publico,
    lado_pontuador_evento_publico as _lado_pontuador_evento_publico,
    normalizar_acao_publica as _normalizar_acao_publica,
    evento_eh_acao_negativa_adversario_publico as _evento_eh_acao_negativa_adversario_publico,
    lado_responsavel_evento_publico as _lado_responsavel_evento_publico,
    rotulo_fundamento_publico as _rotulo_fundamento_publico,
    descricao_evento_publico as _descricao_evento_publico,
    montar_linha_ponto_publico as _montar_linha_ponto_publico,
)
from routes.utils import exigir_perfil, aplicar_placar_exibicao_partida

from rules.partidas_exibicao import (
    STATUS_FINALIZADO,
    STATUS_AO_VIVO,
    STATUS_PRE_JOGO,
    STATUS_AGUARDANDO,
    to_int_or_none as _to_int_or_none,
    normalizar_url_escudo as _normalizar_url_escudo_tabela,
    mapa_escudos_equipes as _mapa_escudos_equipes,
    buscar_escudo_mapa as _buscar_escudo_mapa,
    quadra_label as _quadra_label,
    status_texto as _status_texto,
    partida_tem_flag_finalizada as _partida_tem_flag_finalizada,
    status_normalizado as _status_normalizado,
    status_exibicao as _status_exibicao,
    partida_esta_finalizada as _partida_esta_finalizada,
    partida_esta_ao_vivo as _partida_esta_ao_vivo,
    partida_conta_como_iniciada as _partida_conta_como_iniciada_para_trava,
    fase_partida_normalizada as _fase_partida_normalizada,
    filtrar_partidas_por_fase as _filtrar_partidas_por_fase,
    montar_parciais as _montar_parciais,
)
from services.competicoes.partidas_exibicao import (
    preparar_partidas as _preparar_partidas_service,
)
from services.competicoes.visualizador_publico import (
    montar_contexto_partida_publica as _montar_contexto_partida_publica_service,
    montar_estado_leve_partida_publica as _montar_estado_leve_partida_publica_service,
)
from services.competicoes.tabela_acoes import (
    adicionar_equipe_grupo as _acao_adicionar_equipe_grupo,
    atualizar_partida_manual as _acao_atualizar_partida_manual,
    criar_partida_manual as _acao_criar_partida_manual,
    excluir_grupo as _acao_excluir_grupo,
    excluir_partida as _acao_excluir_partida,
    limpar_fase as _acao_limpar_fase,
    limpar_tabela as _acao_limpar_tabela,
    remover_equipe_grupo as _acao_remover_equipe_grupo,
    vincular_grupo_quadra as _acao_vincular_grupo_quadra,
)
from rules.grupos_estrutura import (
    estrutura_grupo_unico as _estrutura_grupo_unico,
    nomes_grupos_automaticos as _nomes_grupos_automaticos,
    qtd_grupos_configurada as _qtd_grupos_configurada,
)
from services.competicoes.grupos_estrutura import (
    garantir_grupos_estrutura as _garantir_grupos_estrutura_service,
    sincronizar_grupo_unico as _sincronizar_grupo_unico_service,
    sortear_equipes as _sortear_equipes_grupos_service,
)
from services.competicoes.tabela_contexto import (
    contexto_base as _contexto_base_tabela,
    montar_pacote_aba as _montar_pacote_aba_tabela,
    normalizar_aba as _normalizar_aba_tabela,
    normalizar_fase as _normalizar_fase_tabela,
)

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
    """Lê a configuração sem criar ou regravar dados ao abrir a tabela."""
    chave = _cache_key("config_agenda", competicao_nome)
    cached = _cache_get_tabela(chave)
    if cached is not None:
        return cached
    configuracao = buscar_configuracao_agenda_competicao(competicao_nome)
    return _cache_set_tabela(chave, configuracao)


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












# Fachadas internas preservam os nomes históricos da rota enquanto a lógica
# de apresentação vive nos módulos rules/services.
_status_tabela_para_trava = _status_normalizado

def _preparar_partidas(partidas, mapa_escudos=None, competicao=None):
    return _preparar_partidas_service(
        partidas,
        mapa_escudos=mapa_escudos,
        competicao=competicao,
        aplicar_placar_exibicao=aplicar_placar_exibicao_partida,
    )

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
    """Fachada temporária para o serviço de persistência em lote."""
    return _inserir_partidas_em_lote_service(
        partidas, buscar_colunas_tabela=_buscar_colunas_tabela
    )














    

# Regras de apresentação do visualizador público foram extraídas para um
# módulo puro e testável. Os aliases preservam os nomes internos existentes.

def _contexto_partida_publica(competicao_nome, partida_id):
    """Fachada temporária para o serviço do visualizador público."""
    return _montar_contexto_partida_publica_service(
        competicao_nome,
        partida_id,
        _preparar_partidas,
    )


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
    partidas = listar_partidas_leve(competicao_nome, limite=500, incluir_escudos=False) or []
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
    """Estado leve para atualização frequente do placar público."""
    payload = _montar_estado_leve_partida_publica_service(
        competicao_nome,
        partida_id,
        _preparar_partidas,
    )
    if not payload:
        return jsonify({"ok": False, "erro": "Partida não encontrada."}), 404

    resposta = jsonify(payload)
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


def _sincronizar_grupo_unico_automatico(competicao):
    return _sincronizar_grupo_unico_service(
        competicao,
        fase_travada=fase_grupos_esta_travada_por_jogo,
        limpar_cache=_limpar_cache_tabela,
    )


def _garantir_grupos_da_estrutura(competicao):
    return _garantir_grupos_estrutura_service(
        competicao,
        fase_travada=fase_grupos_esta_travada_por_jogo,
        limpar_cache=_limpar_cache_tabela,
    )


def _sortear_equipes_nos_grupos(competicao):
    resultado = _sortear_equipes_grupos_service(
        competicao,
        fase_travada=fase_grupos_esta_travada_por_jogo,
        limpar_cache=_limpar_cache_tabela_e_classificacao,
    )
    return {"ok": resultado.ok, "mensagem": resultado.mensagem}


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

    aba = _normalizar_aba_tabela(request.args.get("aba"))
    fase_subaba = _normalizar_fase_tabela(
        request.args.get("fase"),
        _fase_subaba_canonica,
    )

    # Base leve: dados que a navegação superior e travas usam em qualquer aba.
    # O restante só é carregado conforme a aba ativa. Isso evita que abrir
    # "Configurações" carregue classificação, partidas e avanço completos.
    nome_competicao = competicao["nome"]
    _garantir_grupos_da_estrutura(competicao)
    fases = _fases_disponiveis(competicao)
    grupos_travados = _grupos_travados_cache(nome_competicao)
    fase_banco_ativa = _fase_subaba_para_banco(fase_subaba)
    fase_atual_travada = _fase_atual_travada_cache(nome_competicao, fase_banco_ativa)

    contexto = _contexto_base_tabela(
        competicao=competicao,
        aba=aba,
        fase_subaba=fase_subaba,
        fase_labels=FASES_AVANCO_LABELS,
        fases_disponiveis=fases,
        competicao_travada=_competicao_travada_cache(nome_competicao),
        grupos_travados=grupos_travados,
        fase_atual_travada=fase_atual_travada,
        fase_banco_ativa=fase_banco_ativa,
    )

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

    provedores_contexto = {
        "quadras": _quadras_cache,
        "grupos": _listar_grupos_cache,
        "equipes": _listar_equipes_competicao_cache,
        "grupos_com_equipes": _grupos_com_equipes_cacheados,
        "config_agenda": _config_agenda_cache,
        "estrutura_grupo_unico": _estrutura_grupo_unico,
        "avanco": _avanco_cache,
        "status_avanco": _status_avanco_cache,
        "avanco_gerado": _avanco_gerado_cache,
        "fases_avanco": _fases_do_avanco_para_tabela,
        "series_avanco": _series_do_avanco_por_fase,
        "mapa_escudos": _mapa_escudos_equipes,
        "listar_partidas_frescas": listar_partidas,
        "partida_eh_avanco": _partida_eh_avanco,
        "preparar_partidas": _preparar_partidas,
        "filtrar_partidas_fase": _filtrar_partidas_por_fase,
        "filtrar_partidas_serie": _filtrar_partidas_por_serie_avanco,
        "montar_espelho_avanco": _montar_espelho_avanco,
        "partidas_cache": _listar_partidas_cache,
        "calcular_classificacao": _calcular_ou_obter_classificacao_cacheada,
        "regras_classificacao": _obter_regras_classificacao,
        "criterios_classificacao": _criterios_efetivos_ate_sorteio,
        "colunas_classificacao": _colunas_classificacao_por_criterios,
        "garantir_codigo_publico": garantir_codigo_publico_competicao,
        "url_publico_curto": lambda codigo: url_for(
            "tabela.visualizador_publico_curto",
            codigo_publico=codigo,
        ),
        "url_publico_fallback": lambda nome: url_for(
            "tabela.visualizador_publico",
            competicao_nome=nome,
        ),
    }
    pacote_contexto = _montar_pacote_aba_tabela(
        aba=aba,
        competicao=competicao,
        nome_competicao=nome_competicao,
        fase_subaba=fase_subaba,
        serie_param=serie_param_cache,
        host_url=request.host_url,
        provedores=provedores_contexto,
    )

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
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _acao_vincular_grupo_quadra(
        competicao,
        request.form.get("grupo_nome"),
        request.form.get("quadra_id"),
        fase_grupos_travada=fase_grupos_esta_travada_por_jogo,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/adicionar-equipe", methods=["POST"])
@exigir_organizador_da_competicao
def adicionar_equipe_grupo():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _acao_adicionar_equipe_grupo(
        competicao,
        request.form.get("grupo_id"),
        request.form.get("equipe"),
        fase_grupos_travada=fase_grupos_esta_travada_por_jogo,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/remover-equipe-grupo", methods=["POST"])
@exigir_organizador_da_competicao
def remover_equipe_grupo_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _acao_remover_equipe_grupo(
        competicao,
        request.form.get("grupo_id"),
        request.form.get("equipe"),
        fase_grupos_travada=fase_grupos_esta_travada_por_jogo,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/excluir-grupo/<int:grupo_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_grupo_view(grupo_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _acao_excluir_grupo(
        competicao,
        grupo_id,
        fase_grupos_travada=fase_grupos_esta_travada_por_jogo,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/limpar", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_tabela():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = _acao_limpar_tabela(
        competicao,
        fase_grupos_travada=fase_grupos_esta_travada_por_jogo,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/limpar-fase", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_fase_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    resultado = _acao_limpar_fase(
        competicao,
        _fase_subaba_para_banco(fase_subaba),
        fase_pode_ser_alterada=_fase_pode_ser_alterada_sem_travar_mata_mata,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


@tabela_bp.route("/tabela/nova-partida", methods=["POST"])
@exigir_organizador_da_competicao
def nova_partida():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    resultado = _acao_criar_partida_manual(
        competicao,
        {
            "grupo": request.form.get("grupo"),
            "equipe_a": request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante"),
            "equipe_b": request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante"),
            "fase": _fase_subaba_para_banco(fase_subaba),
            "fase_subaba": fase_subaba,
            "quadra_id": request.form.get("quadra_id"),
        },
        fase_pode_ser_alterada=_fase_pode_ser_alterada_sem_travar_mata_mata,
        estrutura_grupo_unico=_estrutura_grupo_unico,
        sincronizar_grupo_unico=_sincronizar_grupo_unico_automatico,
        listar_grupos=listar_grupos,
        quadra_padrao_grupo=_quadra_padrao_do_grupo,
        listar_partidas=listar_partidas,
        criar_partida=_criar_partida_para_tabela,
        obter_proxima_ordem=proxima_ordem_partida,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


@tabela_bp.route("/tabela/atualizar-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def atualizar_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    resultado = _acao_atualizar_partida_manual(
        competicao,
        partida_id,
        {
            "equipe_a": request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante"),
            "equipe_b": request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante"),
            "fase": _fase_subaba_para_banco(fase_subaba),
            "fase_subaba": fase_subaba,
            "quadra_id": request.form.get("quadra_id"),
            "data_hora": request.form.get("data_hora"),
            "rodada": request.form.get("rodada"),
        },
        buscar_partida=buscar_partida_por_id,
        fase_pode_ser_alterada=_fase_pode_ser_alterada_sem_travar_mata_mata,
        dados_quadra=_dados_quadra,
        atualizar_partida=atualizar_partida,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


@tabela_bp.route("/tabela/excluir-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    resultado = _acao_excluir_partida(
        competicao,
        partida_id,
        excluir=excluir_partida_banco,
    )
    flash(resultado.mensagem, resultado.categoria)
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


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
            return [
                p for p in (lista or [])
                if _origem_partida_avanco(p)[0] == serie_ativa
            ]

        classificacao = None
        if fase_banco in {"quartas", "semifinal"}:
            quartas_existentes = _filtrar_serie_atual(
                _filtrar_partidas_por_fase(partidas_preparadas, "quartas")
            )
            if fase_banco == "quartas" or len(quartas_existentes) < 4:
                grupos = _grupos_com_equipes_cacheados(nome_competicao, grupos_raw, incluir_quadra=False)
                classificacao, _ = _calcular_ou_obter_classificacao_cacheada(
                    nome_competicao, partidas_preparadas, grupos, competicao, mapa_escudos
                )
        else:
            quartas_existentes = []

        semifinais_existentes = _filtrar_serie_atual(
            _filtrar_partidas_por_fase(partidas_preparadas, "semifinais")
        ) if fase_banco in {"final", "terceiro_lugar"} else []

        ordem_inicial = max(
            [int(p.get("ordem") or 0) for p in partidas if p.get("ordem") is not None] or [0]
        ) + 1
        quadra_id, quadra_nome = _quadra_nome_cache(
            mapa_quadras, _to_int_or_none(request.form.get("quadra_id"))
        )

        resultado_mata_mata = gerar_e_persistir_mata_mata(
            fase=fase_banco,
            nome_competicao=nome_competicao,
            serie=serie_ativa,
            classificacao=classificacao,
            quartas=quartas_existentes,
            semifinais=semifinais_existentes,
            resolver_vencedor=lambda partida, placeholder: _vencedor_partida_rapido(partida, placeholder),
            remover_pendentes=lambda: _limpar_partidas_fase_serie_nao_iniciadas(
                nome_competicao, fase_banco, serie_ativa
            ),
            ordem_inicial=ordem_inicial,
            quadra_id=quadra_id,
            quadra_nome=quadra_nome,
            buscar_data_hora=lambda indice: buscar_data_hora_rodada_programada(
                nome_competicao, "avanco", fase_banco, serie_ativa, indice
            ),
            buscar_colunas_tabela=_buscar_colunas_tabela,
        )

        if not resultado_mata_mata.get("ok"):
            flash(resultado_mata_mata.get("mensagem") or "Não foi possível montar confrontos automáticos para esta fase.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba, serie=serie_ativa))

        removidas = resultado_mata_mata.get("removidas", 0)
        total_inserido = resultado_mata_mata.get("inseridas", 0)
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

