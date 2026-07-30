from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify, make_response
import threading
import time
import os
import copy
from datetime import datetime
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
    buscar_partida_operacional,
    listar_arbitros_competicao,
    salvar_pre_jogo_partida,
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
    normalizar_status_partidas_apontador,
    salvar_estado_manual_partida,
    salvar_resultado_manual_partida,
    salvar_liberos_equipe,
    atualizar_atleta_conferencia_apontador,
    listar_dados_finalizacao_partida,
    salvar_destaque_partida,
    buscar_config_destaques_competicao,
    verificar_destaques_competicao_pendentes,
    listar_respostas_destaques_competicao,
    listar_atletas_para_destaques_competicao,
    salvar_respostas_destaques_competicao,
    gerar_partidas_avanco_competicao,
    listar_rodadas_competicao,
    finalizar_partida_completa,
)
from services.atletas.consultas import listar_atletas_aprovados_da_equipe
from services.competicoes.partidas import listar_partidas_leve
from services.apontadores.pre_jogo import (
    montar_contexto_pre_jogo as _montar_contexto_pre_jogo,
    validar_acesso_operador as _validar_acesso_operador_pre_jogo,
    resolver_equipe_conferencia as _resolver_equipe_conferencia,
    preparar_alteracoes_numeracao as _preparar_alteracoes_numeracao,
    contexto_capitao as _contexto_capitao,
)
from services.apontadores.painel import (
    resolver_cpf_sessao as _resolver_cpf_sessao_painel,
    contexto_home as _contexto_home_apontador,
    preparar_partidas_painel as _preparar_partidas_painel,
)
from services.apontadores.operador import (
    abandonar_partida as _abandonar_partida_operador,
    assumir_partida as _assumir_partida_operador,
    buscar_vinculo_por_pin as _buscar_vinculo_operacional_por_pin,
    garantir_pin as _garantir_pin_operacional_apontador,
    heartbeat_partida as _heartbeat_partida_operador,
    liberar_partida as _liberar_partida_operador,
    resolver_login_sessao as _resolver_login_apontador,
    validar_operador as _validar_operador_partida,
    validar_schema_oficiais as _validar_schema_oficiais_apontador,
)
from rules.papeleta import (
    fase_exige_correcao as _fase_papeleta_exige_correcao,
    set_operacional_seguro as _set_atual_operacional_seguro,
)
from services.apontadores.papeleta import (
    montar_contexto_papeleta as _montar_contexto_papeleta,
    montar_estado_inicial_jogo as _montar_estado_inicial_jogo,
    papeletas_set_completas as _papeletas_set_completas,
    preparar_escalacao as _preparar_escalacao_papeleta,
    valores_formulario as _valores_formulario_papeleta,
)

from services.apontadores.estado_jogo import carregar_contexto_jogo as _carregar_contexto_jogo
from services.apontadores.acoes_jogo import (
    ErroAcaoJogo as _ErroAcaoJogo,
    aplicar_local as _aplicar_acao_extra_local,
    descrever as _descrever_acao_extra,
    preparar_cartao_verde as _preparar_cartao_verde,
    preparar_retardamento as _preparar_retardamento,
    preparar_sancao as _preparar_sancao,
    preparar_tempo as _preparar_tempo,
)
from rules.pontos_jogo import ErroPonto
from services.apontadores.pontos import (
    completar_estado_registrado as _completar_estado_ponto,
    montar_resposta_ponto as _montar_resposta_ponto,
    preparar_registro_ponto as _preparar_registro_ponto,
    publicar_ponto as _publicar_ponto,
    set_ou_jogo_finalizado as _set_ou_jogo_finalizado,
)
from services.apontadores.rotacao import rotacao_do_estado as _rotacao_segura_estado
from services.apontadores.substituicoes import (
    ErroSubstituicao as _ErroSubstituicao,
    aplicar_estado_visual as _aplicar_substituicao_visual,
    validar_comando as _validar_comando_substituicao,
)
from services.apontadores.responses import json_no_cache as _resposta_json_no_cache
from services.apontadores.publicacao import (
    publicar_estado as _publicar_estado_apontador,
    publicar_estado_sem_cache as _publicar_estado_apontador_sem_cache,
)
from services.relatorios.cache import invalidar_cache_competicao
from services.apontadores.finalizacao import (
    confirmar_sets as _confirmar_sets_finalizacao,
    contexto_observacoes as _contexto_observacoes_finalizacao,
    estado_partida_finalizada as _estado_partida_finalizada,
    eventos_processados_com_sucesso as _eventos_finalizacao_ok,
    preparar_estado_cliente as _preparar_estado_final_cliente,
    preparar_formulario_finalizacao as _preparar_formulario_finalizacao,
    resposta_entre_sets as _resposta_finalizacao_entre_sets,
    resposta_partida_finalizada as _resposta_finalizacao_concluida,
    separar_eventos_pendentes as _separar_eventos_finalizacao,
)
from routes.utils import exigir_perfil, aplicar_placar_exibicao_lista, aplicar_placar_exibicao_partida
from socket_events import (
    emitir_estado_partida,
    emitir_placar_apontador,
    obter_estado_cache,
    atualizar_estado_cache,
    emitir_tempo_executado,
    emitir_substituicao_executada,
    emitir_resposta_solicitacao,
)


try:
    from routes.offline_config import offline_global_habilitado
except Exception:
    def offline_global_habilitado():
        return False

from services.apontadores.avanco import atualizar as _executar_avanco, atualizar_async as _executar_avanco_async
from services.apontadores.cache_runtime import caches_apontador as _caches_apontador
from services.apontadores.operacao_local import operacao_local_store as _operacao_local_store
from services.apontadores.configuracao import (
    buscar_competicao as _buscar_competicao_config,
    buscar_configuracao_avancada as _buscar_configuracao_avancada,
    limites_operacionais as _limites_operacionais_config,
    normalizar_fase as _normalizar_fase_config,
    resolver_modo_operacao as _resolver_modo_operacao_config,
    sets_max as _sets_max_config,
    sets_para_vencer as _sets_para_vencer_config,
)


def _atualizar_avanco_apos_finalizacao(competicao):
    return _executar_avanco(
        competicao,
        gerar=gerar_partidas_avanco_competicao,
    )


def _atualizar_avanco_apos_finalizacao_async(competicao):
    return _executar_avanco_async(
        competicao,
        gerar=gerar_partidas_avanco_competicao,
        ao_concluir=lambda nome: _limpar_cache_apontador(nome),
    )


apontadores_bp = Blueprint("apontadores", __name__)

_CACHE_ARBITROS_COMPETICAO = {}
_CACHE_ATLETAS_EQUIPE = {}
_CACHE_MODO_OPERACAO = {}
_CACHE_COLUNAS_ESCUDO_EQUIPE = {"valor": None}
_TABELAS_OFICIAIS_GARANTIDAS = False


def _chave_operacao_local(partida_id, competicao):
    return _operacao_local_store.chave(partida_id, competicao)


def _salvar_snapshot_operacao_local(partida_id, competicao, partida=None, **extras):
    return _operacao_local_store.salvar(
        partida_id, competicao, partida=partida, **extras
    )


def _snapshot_operacao_local(partida_id, competicao):
    return _operacao_local_store.obter(partida_id, competicao)


def _partida_operacao_local(partida_id, competicao):
    return _operacao_local_store.partida(partida_id, competicao)


def _buscar_competicao_cache(competicao):
    return _buscar_competicao_config(competicao)

def _cache_get(chave, ttl=None):
    return _caches_apontador.painel.obter(chave, ttl=ttl)


def _cache_set(chave, valor):
    return _caches_apontador.painel.salvar(chave, valor)


def _limpar_cache_painel_competicao(competicao=None):
    _caches_apontador.limpar_painel_competicao(competicao)


def _limpar_cache_apontador(competicao=None, cpf=None):
    _caches_apontador.limpar_operacao(competicao=competicao, cpf=cpf)


def _montar_home_apontador_cache(cpf):
    return _caches_apontador.montar_home(
        cpf=cpf,
        cliente_id=session.get("cliente_id"),
        pode_jogo_avulso=apontador_pode_criar_jogo_avulso,
        buscar_oficial=buscar_oficial_por_cpf,
        listar_competicoes=listar_competicoes_apontador,
        offline_habilitado=offline_global_habilitado,
    )


def _garantir_pin_operacional_cache(competicao, login):
    return _caches_apontador.garantir_pin(
        competicao=competicao,
        login=login,
        gerar_pin=_garantir_pin_operacional_apontador,
    )


def _garantir_tabelas_oficiais_once():
    """Valida o schema sem executar DDL durante a requisição."""
    global _TABELAS_OFICIAIS_GARANTIDAS
    if _TABELAS_OFICIAIS_GARANTIDAS:
        return
    _validar_schema_oficiais_apontador()
    _TABELAS_OFICIAIS_GARANTIDAS = True



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
    return _normalizar_fase_config(fase)

def _resolver_modo_operacao_partida(competicao, partida=None):
    return _resolver_modo_operacao_config(competicao, partida)

def _sets_max_competicao(competicao):
    return _sets_max_config(competicao)


def _sets_para_vencer_competicao(competicao):
    return _sets_para_vencer_config(competicao)

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
    return _resolver_login_apontador(session)



def _validar_operador_http(partida_id, competicao, renovar=True):
    """Proteção central: só o apontador que assumiu a partida pode operar."""
    login = _login_apontador_sessao()
    ok, msg, partida = _validar_operador_partida(partida_id, competicao, login, renovar=renovar)
    return ok, msg, partida


def _erro_operador_json(msg, status=423):
    return _json_no_cache({
        "ok": False,
        "bloqueada": True,
        "mensagem": msg or "Esta partida está em operação por outro apontador.",
    }, status)



def _partida_em_sorteio_tiebreak(partida=None):
    """Detecta quando o próximo passo é o sorteio exclusivo do tie-break.

    O fluxo oficial pode marcar isso em campos diferentes dependendo da origem
    da transição (fim do set, pré-jogo, cache antigo). Por isso centralizamos a
    checagem para não voltar para o pré-jogo com um botão intermediário.
    """
    partida = partida or {}
    campos = {
        str(partida.get("fase_partida") or "").strip().lower(),
        str(partida.get("status_jogo") or "").strip().lower(),
        str(partida.get("status_operacao") or "").strip().lower(),
    }
    if campos.intersection({"tiebreak_sorteio", "tie_break_sorteio", "sorteio_tiebreak", "sorteio_tie_break"}):
        return True
    return bool(partida.get("tiebreak_pendente")) and not bool(partida.get("tiebreak_definido"))



def _numero_set_tiebreak_partida(partida=None, competicao=None):
    """Retorna o número do set decisivo da partida.

    Melhor de 3 -> set 3.
    Melhor de 5 -> set 5.
    """
    partida = partida or {}
    try:
        sets_max = int(partida.get("sets_max") or 0)
    except Exception:
        sets_max = 0

    if not sets_max:
        try:
            sets_max = int(_sets_max_competicao(competicao))
        except Exception:
            sets_max = 3

    return 5 if sets_max >= 5 else 3


def _equipe_oposta_tiebreak(partida=None, equipe_referencia=""):
    partida = partida or {}
    ref = str(equipe_referencia or "").strip()

    candidatos = [
        partida.get("equipe_a"),
        partida.get("equipe_b"),
        partida.get("equipe_a_operacional"),
        partida.get("equipe_b_operacional"),
    ]

    vistos = []
    for nome in candidatos:
        nome = str(nome or "").strip()
        if nome and nome not in vistos:
            vistos.append(nome)

    for nome in vistos:
        if nome.lower() != ref.lower():
            return nome

    return ""


def _colunas_partidas_cur(cur):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'partidas'
    """)
    return {str(row.get("column_name") or "").strip() for row in (cur.fetchall() or [])}


def _atualizar_partida_campos_existentes(cur, partida_id, competicao, valores):
    """Atualiza somente colunas existentes em partidas para evitar erro em bancos antigos."""
    colunas = _colunas_partidas_cur(cur)
    sets = []
    params = []

    for campo, valor in (valores or {}).items():
        if campo in colunas:
            sets.append(f"{campo} = %s")
            params.append(valor)

    if not sets:
        return False

    params.extend([partida_id, competicao])
    cur.execute(f"""
        UPDATE partidas
        SET {", ".join(sets)}
        WHERE id = %s
          AND competicao = %s
    """, tuple(params))
    return True


def _salvar_sorteio_tiebreak_direto(partida_id, competicao, operador_login, partida,
                                    sorteio_vencedor, sorteio_escolha,
                                    saque_tiebreak, lado_esquerdo_tiebreak):
    """Salva o sorteio do tie-break e libera diretamente a papeleta do set decisivo.

    Essa rotina é intencionalmente tolerante: se a função do banco recusar dizendo
    que o tie-break não está liberado, esta rota ainda consegue gravar quando a
    partida já chegou à tela específica do tie-break.
    """
    partida = dict(partida or {})

    sorteio_vencedor = str(sorteio_vencedor or "").strip()
    sorteio_escolha = str(sorteio_escolha or "").strip()
    saque_tiebreak = str(saque_tiebreak or "").strip()
    lado_esquerdo_tiebreak = str(lado_esquerdo_tiebreak or "").strip()

    if not sorteio_vencedor:
        return False, "Selecione a equipe vencedora do sorteio."
    if not saque_tiebreak:
        return False, "Selecione a equipe que inicia sacando no tie-break."
    if not lado_esquerdo_tiebreak:
        return False, "Selecione a equipe que ficará no lado esquerdo no tie-break."

    equipe_direita_tiebreak = _equipe_oposta_tiebreak(partida, lado_esquerdo_tiebreak)
    if not equipe_direita_tiebreak:
        return False, "Não consegui identificar a equipe do lado direito do tie-break."

    set_tiebreak = _numero_set_tiebreak_partida(partida, competicao)
    saque_atual = "A" if saque_tiebreak.strip().lower() == lado_esquerdo_tiebreak.strip().lower() else "B"

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                _atualizar_partida_campos_existentes(cur, partida_id, competicao, {
                    "set_atual": set_tiebreak,
                    "pontos_a": 0,
                    "pontos_b": 0,

                    "status": "ao vivo",
                    "status_jogo": "papeleta",
                    "fase_partida": "papeleta",
                    "status_operacao": "papeleta",

                    "tiebreak_pendente": False,
                    "tiebreak_definido": True,
                    "sorteio_tiebreak_vencedor": sorteio_vencedor,
                    "sorteio_tiebreak_escolha": sorteio_escolha,
                    "saque_tiebreak": saque_tiebreak,
                    "lado_esquerdo_tiebreak": lado_esquerdo_tiebreak,

                    "sorteio_vencedor": sorteio_vencedor,
                    "sorteio_escolha": sorteio_escolha,
                    "saque_inicial": saque_tiebreak,
                    "saque_atual": saque_atual,

                    "lado_esquerdo": lado_esquerdo_tiebreak,
                    "lado_direito": equipe_direita_tiebreak,
                    "equipe_a_operacional": lado_esquerdo_tiebreak,
                    "equipe_b_operacional": equipe_direita_tiebreak,

                    "rotacao_a_json": None,
                    "rotacao_b_json": None,
                    "titulares_iniciais_a_json": None,
                    "titulares_iniciais_b_json": None,

                    "pre_jogo_finalizado": True,
                    "pre_jogo_finalizado_em": datetime.now(),
                    "operador_login": operador_login,
                    "apontador_login": operador_login,
                })
            conn.commit()

        return True, "Sorteio do tie-break salvo. Preencha a papeleta do set decisivo."

    except Exception as e:
        print("ERRO salvar sorteio tie-break direto:", repr(e), flush=True)
        return False, "Erro ao salvar o sorteio do tie-break."



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
    return _resposta_json_no_cache(payload, status)


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
    return _limites_operacionais_config(partida, estado)

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
        comp = _buscar_competicao_cache(competicao) or {}
        # As regras da competição são a fonte oficial. Elas devem substituir
        # valores-padrão já existentes na partida/cache (por exemplo 2 tempos),
        # senão o modo local nasce com uma regra diferente da definida pelo organizador.
        aliases_regra = {
            "tempos_por_set": ("tempos_por_set", "limite_tempos", "tempos_limite"),
            "substituicoes_por_set": ("substituicoes_por_set", "limite_substituicoes", "substituicoes_limite"),
            "pontos_set": ("pontos_set", "ponto_alvo_set", "pontos_para_vencer_set"),
            "pontos_tiebreak": ("pontos_tiebreak",),
            "diferenca_minima": ("diferenca_minima",),
            "sets_tipo": ("sets_tipo",),
        }
        for destino, fontes in aliases_regra.items():
            valor = next((comp.get(c) for c in fontes if comp.get(c) is not None and comp.get(c) != ""), None)
            if valor is not None:
                partida[destino] = valor
                estado[destino] = valor
        if partida.get("tempos_por_set") is not None:
            partida["limite_tempos"] = partida.get("tempos_por_set")
            estado["limite_tempos"] = partida.get("tempos_por_set")
        if partida.get("substituicoes_por_set") is not None:
            partida["limite_substituicoes"] = partida.get("substituicoes_por_set")
            estado["limite_substituicoes"] = partida.get("substituicoes_por_set")
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

    # IMPORTANTE: o modo do scout também precisa ir dentro do estado/cache.
    # Algumas telas e respostas via socket leem estado.modo_operacao, não apenas
    # a variável modo_operacao enviada no render_template. Se o cache nasceu como
    # simples, ele sobrescrevia a partida avançada e o jogo abria sem scout.
    try:
        modo_resolvido = str(
            partida.get("modo_operacao_resolvido")
            or _resolver_modo_operacao_partida(competicao, partida)
            or partida.get("modo_operacao")
            or estado.get("modo_operacao")
            or "simples"
        ).strip().lower()
    except Exception:
        modo_resolvido = str(partida.get("modo_operacao") or estado.get("modo_operacao") or "simples").strip().lower()

    if modo_resolvido not in {"simples", "avancado"}:
        modo_resolvido = "simples"

    estado["modo_operacao"] = modo_resolvido
    estado["modo_operacao_resolvido"] = modo_resolvido
    estado["permite_scout"] = modo_resolvido == "avancado"

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
    """Retorna colunas possíveis de escudo com cache.

    Essa função era chamada ao abrir o jogo e fazia consulta em
    information_schema toda vez. No celular, principalmente vindo da papeleta,
    isso somava atraso desnecessário antes da tela do apontador abrir.
    """
    if _CACHE_COLUNAS_ESCUDO_EQUIPE.get("valor") is not None:
        return _CACHE_COLUNAS_ESCUDO_EQUIPE.get("valor") or []

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
    colunas = [c for c in ordem if c in existentes]
    _CACHE_COLUNAS_ESCUDO_EQUIPE["valor"] = colunas
    return colunas


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

    # equipe_a/equipe_b oficiais da partida nunca devem ser invertidas.
    # equipe_a_operacional/equipe_b_operacional representam o lado atual da quadra.
    estado["equipe_a_cadastro"] = partida.get("equipe_a") or estado.get("equipe_a_cadastro") or equipe_a_op
    estado["equipe_b_cadastro"] = partida.get("equipe_b") or estado.get("equipe_b_cadastro") or equipe_b_op
    estado["equipe_a_operacional"] = equipe_a_op
    estado["equipe_b_operacional"] = equipe_b_op
    estado["equipe_a"] = estado["equipe_a_cadastro"]
    estado["equipe_b"] = estado["equipe_b_cadastro"]
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

    return _publicar_estado_apontador(
        partida_id=partida_id,
        estado=estado,
        atualizar_cache=atualizar_estado_cache,
        emitir_estado=emitir_estado_partida,
        apontador_login=apontador_login,
        emitir_placar=emitir_placar_apontador,
        origem=origem,
    )


# =========================================================
# CONSULTAS BÁSICAS
# =========================================================
def listar_apontadores():
    try:
        garantir_coluna_jogo_avulso_apontador()
    except Exception as e:
        print("ERRO garantir coluna jogo avulso:", e, flush=True)

    cliente_id = session.get("cliente_id")
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(TRIM(o.nome), ''), 'Apontador sem nome') AS nome,
                    REGEXP_REPLACE(COALESCE(a.cpf, o.cpf, ''), '\\D', '', 'g') AS cpf,
                    a.ativo,
                    a.primeiro_acesso,
                    COALESCE(a.pode_criar_jogo_avulso, FALSE) AS pode_criar_jogo_avulso,
                    a.cliente_id
                FROM apontadores_acesso a
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') =
                     REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g')
                 AND o.cliente_id = a.cliente_id
                WHERE (%s::INTEGER IS NULL OR a.cliente_id = %s)
                ORDER BY COALESCE(NULLIF(TRIM(o.nome), ''), 'Apontador sem nome')
            """, (cliente_id, cliente_id))
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
            cliente_id = session.get("cliente_id")
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT cpf
                        FROM oficiais
                        WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                          AND (%s::INTEGER IS NULL OR cliente_id = %s)
                        LIMIT 1
                    """, (cpf_limpo, cliente_id, cliente_id))
                    oficial_existente = cur.fetchone()

            if oficial_existente and oficial_existente.get("cpf"):
                with conectar() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE oficiais
                               SET nome = COALESCE(NULLIF(%s, ''), nome),
                                   cpf = %s
                             WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                               AND (%s::INTEGER IS NULL OR cliente_id = %s)
                        """, (nome, cpf_limpo, cpf_limpo, cliente_id, cliente_id))
                    conn.commit()
            else:
                cadastrar_oficial(nome, cpf_limpo, cliente_id=cliente_id)

            criar_apontador(cpf_limpo, cliente_id=cliente_id)
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
    # Não roda criação/verificação de tabelas aqui. Essa tela precisa ser leve.
    # O schema deve ser garantido no startup/deploy ou por rota administrativa.
    #
    # IMPORTANTE:
    # A tela do apontador precisa localizar o vínculo pelo CPF real.
    # _login_apontador_sessao() pode retornar login/nome da sessão, então aqui
    # priorizamos os campos de CPF e só usamos outros campos se eles tiverem
    # 11 dígitos. Isso corrige o erro "Não foi possível localizar o apontador".
    cpf = _resolver_cpf_sessao_painel(
        session,
        somente_digitos,
        _login_apontador_sessao() or "",
    )

    try:
        dados_home = _montar_home_apontador_cache(cpf)
    except Exception as e:
        print("ERRO montar home apontador:", repr(e), flush=True)
        dados_home = {
            "cpf": cpf,
            "oficial": None,
            "competicoes": [],
            "pode_jogo_avulso": False,
            "offline_habilitado": False,
        }

    contexto = _contexto_home_apontador(dados_home)

    if not cpf:
        flash("CPF do apontador não encontrado na sessão.", "erro")
        return render_template("painel_apontador.html", **contexto)

    if not dados_home.get("oficial"):
        flash("Não foi possível localizar o apontador pelo CPF informado.", "erro")
        return render_template("painel_apontador.html", **contexto)

    return render_template("painel_apontador.html", **contexto)



def _montar_partidas_painel_apontador_cache(competicao):
    """Monta lista leve da competição para a tela do apontador.

    Não carrega atletas, papeleta, eventos nem evolução ponto a ponto.
    Essa tela só precisa listar jogos e placar resumido.
    """
    competicao = str(competicao or "").strip()
    chave = ("painel_competicao", competicao, "v5-rodadas-organizador")
    cached = _cache_get(chave)
    if cached is not None:
        return cached

    competicao_cfg = _buscar_competicao_config(competicao) or {"nome": competicao, "sets_tipo": "melhor_de_3"}

    try:
        config_avancada = _buscar_configuracao_avancada(competicao) or {}
    except Exception:
        config_avancada = {}

    partidas = listar_partidas_leve(competicao, limite=1000) or []

    # Mantém a normalização existente, mas só uma vez por cache curto.
    try:
        partidas = normalizar_status_partidas_apontador(partidas, competicao)
    except Exception as e:
        print("AVISO normalizar partidas apontador:", repr(e), flush=True)

    try:
        partidas = aplicar_placar_exibicao_lista(partidas, competicao_cfg)
    except Exception as e:
        print("AVISO placar exibicao lista apontador:", repr(e), flush=True)

    try:
        rodadas_configuradas = listar_rodadas_competicao(competicao) or []
    except Exception:
        rodadas_configuradas = []

    payload = _preparar_partidas_painel(
        partidas=partidas,
        competicao_cfg=competicao_cfg,
        config_avancada=config_avancada,
        rodadas_configuradas=rodadas_configuradas,
        sets_max_manual=_sets_max_competicao(competicao),
        normalizar_escudo=_normalizar_url_escudo,
        escudo_padrao=ESCUDO_PADRAO_URL,
    )
    payload["competicao_cfg"] = competicao_cfg
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
        pin_operacional = _garantir_pin_operacional_cache(competicao, _login_apontador_sessao())
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

    try:
        pendencia_destaques_competicao = verificar_destaques_competicao_pendentes(competicao)
    except Exception:
        pendencia_destaques_competicao = {"abrir": False}

    return render_template(
        "painel_apontador.html",
        modo_partidas=True,
        competicao_nome=competicao,
        partidas=dados.get("partidas") or [],
        rodadas_exibicao=dados.get("rodadas_exibicao") or [],
        pin_operacional=pin_operacional,
        pode_jogo_avulso=pode_jogo_avulso,
        offline_habilitado=offline_habilitado,
        sets_max_manual=dados.get("sets_max_manual") or 3,
        sets_para_vencer_manual=dados.get("sets_para_vencer_manual") or 2,
        pendencia_destaques_competicao=pendencia_destaques_competicao,
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
        comp = _buscar_competicao_config(competicao) or {"competicao": competicao}
        partidas_brutas = listar_partidas_leve(competicao, limite=1000) or []
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
        _limpar_cache_apontador(competicao)
        _atualizar_avanco_apos_finalizacao_async(competicao)
        flash(f"{msg} Avanço será atualizado em segundo plano.", "sucesso")
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

    # Se a partida chegou ao tie-break, não volta para o pré-jogo com botão.
    # O apontador deve cair direto na tela exclusiva de sorteio do tie-break.
    if _partida_em_sorteio_tiebreak(partida):
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    if partida.get("equipe_a_operacional") or partida.get("equipe_b_operacional"):
        try:
            partida = aplicar_capitaes_padrao_partida(partida_id, competicao) or partida
        except Exception:
            pass

    bloqueada_por_outro = False
    if partida.get("operador_login") and partida.get("operador_login") != cpf:
        try:
            ok_lock, msg_lock, _ = _validar_operador_partida(partida_id, competicao, cpf, renovar=False)
            bloqueada_por_outro = (not ok_lock) and ("operação por" in (msg_lock or "").lower())
        except Exception:
            bloqueada_por_outro = (
                partida.get("operador_login")
                and partida.get("operador_login") != cpf
                and (partida.get("status_operacao") or "livre").lower() in {"reservado", "pre_jogo", "em_andamento"}
            )

    arbitros = _listar_arbitros_competicao_cache(competicao)

    # Carga única da operação: partida, regras, árbitros e os dois elencos.
    # Depois do sorteio, conferência/papeleta/jogo usam cache + estado do navegador.
    try:
        atletas_iniciais_a = _listar_atletas_aprovados_cache(partida.get("equipe_a"), competicao) if partida.get("equipe_a") else []
        atletas_iniciais_b = _listar_atletas_aprovados_cache(partida.get("equipe_b"), competicao) if partida.get("equipe_b") else []
    except Exception:
        atletas_iniciais_a, atletas_iniciais_b = [], []
    _salvar_snapshot_operacao_local(
        partida_id, competicao, partida=partida, arbitros=arbitros,
        atletas_por_equipe={
            str(partida.get("equipe_a") or ""): atletas_iniciais_a,
            str(partida.get("equipe_b") or ""): atletas_iniciais_b,
        },
    )

    contexto = _montar_contexto_pre_jogo(
        partida=partida,
        arbitros=arbitros,
        operador_login=cpf,
        equipe_ja_conferida_fn=equipe_ja_conferida,
        competicao=competicao,
        bloqueada_por_outro=bloqueada_por_outro,
    )
    return render_template("pre_jogo_apontador.html", **contexto)


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/assumir", methods=["POST"])
@exigir_perfil("apontador")
def assumir_partida_view(competicao, partida_id):
    operador_login = _login_apontador_sessao()
    cpf_sessao = (session.get("usuario") or session.get("cpf") or operador_login or "").strip()

    oficial = None
    try:
        oficial = buscar_oficial_por_cpf(cpf_sessao, cliente_id=session.get("cliente_id"))
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

    ok, msg = _assumir_partida_operador(
        partida_id,
        competicao,
        operador_login,
        operador_nome
    )

    if ok:
        _limpar_cache_apontador(competicao)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id, rapido="1"))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/abandonar", methods=["POST"])
@exigir_perfil("apontador")
def abandonar_partida_view(competicao, partida_id):
    cpf = _login_apontador_sessao()
    ok, msg = _abandonar_partida_operador(partida_id, competicao, cpf)
    if ok:
        _limpar_cache_apontador(competicao)
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

    if ok:
        _limpar_cache_apontador(competicao)
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

    # Se chegou nesta rota, a tela correta é o sorteio exclusivo do tie-break.
    # Não voltamos mais para o pré-jogo e não bloqueamos por inconsistência antiga
    # de status_operacao/fase_partida.
    fluxo["fase_partida"] = "tiebreak_sorteio"
    fluxo["tiebreak_pendente"] = True
    fluxo["set_atual"] = _numero_set_tiebreak_partida(partida, competicao)

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
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") and partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode salvar o sorteio do tie-break.", "erro")
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    vencedor_sorteio = (
        request.form.get("sorteio_vencedor")
        or request.form.get("vencedor_sorteio")
        or request.form.get("equipe_vencedora")
        or request.form.get("vencedor")
        or ""
    ).strip()

    escolha_sorteio = (
        request.form.get("sorteio_escolha")
        or request.form.get("escolha_sorteio")
        or request.form.get("escolha")
        or ""
    ).strip()

    saque_tiebreak = (
        request.form.get("saque_tiebreak")
        or request.form.get("saque_inicial")
        or request.form.get("equipe_saque")
        or request.form.get("sacador")
        or ""
    ).strip()

    lado_esquerdo_tiebreak = (
        request.form.get("lado_esquerdo_tiebreak")
        or request.form.get("lado_esquerdo")
        or request.form.get("equipe_lado_esquerdo")
        or request.form.get("esquerda")
        or ""
    ).strip()

    # Tenta a rotina oficial do banco, mas NÃO deixa ela travar o ginásio.
    # Em algumas partidas antigas o banco pode responder "tie-break não liberado"
    # mesmo quando a tela correta já é /apontador/tiebreak. A gravação direta abaixo
    # é a fonte final para liberar a papeleta do set decisivo.
    try:
        salvar_sorteio_tiebreak_partida(
            partida_id=partida_id,
            competicao=competicao,
            operador_login=cpf,
            sorteio_vencedor=vencedor_sorteio,
            sorteio_escolha=escolha_sorteio,
            saque_tiebreak=saque_tiebreak,
            lado_esquerdo_tiebreak=lado_esquerdo_tiebreak,
        )
    except Exception as e:
        print("AVISO salvar_tiebreak_view/rotina oficial:", repr(e), flush=True)

    ok, msg = _salvar_sorteio_tiebreak_direto(
        partida_id, competicao, cpf, partida,
        vencedor_sorteio, escolha_sorteio,
        saque_tiebreak, lado_esquerdo_tiebreak
    )

    if ok:
        _limpar_cache_apontador(competicao)
        flash(msg or "Sorteio do tie-break salvo. Preencha a papeleta do set decisivo.", "sucesso")
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    flash(msg or "Erro ao salvar sorteio do tie-break.", "erro")
    return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>")
@exigir_perfil("apontador")
def conferencia_equipe_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    modo_local = request.args.get("local") == "1"
    partida = _partida_operacao_local(partida_id, competicao) if modo_local else {}
    if not partida:
        partida = buscar_partida_operacional(partida_id, competicao)
        if partida:
            _salvar_snapshot_operacao_local(partida_id, competicao, partida=partida)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    ok_acesso, msg_acesso = _validar_acesso_operador_pre_jogo(partida, cpf, "fazer a conferência")
    if not ok_acesso:
        flash(msg_acesso, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    equipe_informada = (request.args.get("equipe") or "").strip() if modo_local else ""
    ok_equipe, msg_equipe, equipe = _resolver_equipe_conferencia(
        partida=partida,
        lado=lado,
        equipe_informada=equipe_informada,
    )
    if not ok_equipe:
        flash(msg_equipe, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    snap = _snapshot_operacao_local(partida_id, competicao)
    atletas = ((snap.get("atletas_por_equipe") or {}).get(equipe) or _listar_atletas_aprovados_cache(equipe, competicao))

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

    ok_acesso, msg_acesso = _validar_acesso_operador_pre_jogo(partida, cpf, "salvar a conferência")
    if not ok_acesso:
        flash(msg_acesso, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    ok_equipe, msg_equipe, equipe = _resolver_equipe_conferencia(partida=partida, lado=lado)
    if not ok_equipe:
        flash(msg_equipe, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    ids = [str(i).strip() for i in request.form.getlist("atleta_id") if str(i).strip()]
    atletas_atuais = listar_atletas_aprovados_da_equipe(equipe, competicao) or []
    valores_numeros = {atleta_id: request.form.get(f"numero_{atleta_id}", "") for atleta_id in ids}
    alteracoes, erros = _preparar_alteracoes_numeracao(
        atletas=atletas_atuais,
        ids=ids,
        valores=valores_numeros,
    )

    if erros:
        for msg in erros:
            flash(msg, "erro")
        return redirect(url_for("apontadores.conferencia_equipe_view", competicao=competicao, partida_id=partida_id, lado=lado))

    houve_erro = False
    mensagens_exibidas = set()

    for atleta_id, numero in alteracoes:
        resultado_numero = atualizar_numero_atleta(atleta_id, "" if numero is None else str(numero))
        if isinstance(resultado_numero, tuple):
            ok, msg = resultado_numero
        else:
            ok = bool(resultado_numero)
            msg = "Não foi possível atualizar a numeração do atleta." if not ok else "Numeração atualizada com sucesso."
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

    ok_acesso, msg_acesso = _validar_acesso_operador_pre_jogo(partida, cpf, "definir o capitão")
    if not ok_acesso:
        flash(msg_acesso, "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    ok_equipe, msg_equipe, equipe = _resolver_equipe_conferencia(partida=partida, lado=lado)
    if not ok_equipe:
        flash("Lado inválido para capitão." if "Lado inválido" in msg_equipe else "Equipe operacional ainda não definida.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = _listar_atletas_aprovados_cache(equipe, competicao)
    contexto = _contexto_capitao(
        partida=partida,
        lado=lado,
        atletas=atletas,
        competicao=competicao,
    )
    return render_template("definir_capitao.html", **contexto)


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_capitao_view(competicao, partida_id, lado):
    cpf = _login_apontador_sessao()
    atleta_id = request.form.get("atleta_id", "").strip()

    ok, msg = salvar_capitao_partida(partida_id, competicao, cpf, lado, atleta_id)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))




def _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida):
    set_seguro = _set_atual_operacional_seguro(partida)
    try:
        set_banco = int((partida or {}).get("set_atual") or 1)
    except Exception:
        set_banco = 1

    if set_seguro != set_banco or _fase_papeleta_exige_correcao(partida):
        try:
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE partidas
                        SET set_atual = %s,
                            fase_partida = 'papeleta',
                            status_jogo = 'papeleta',
                            status_operacao = 'papeleta'
                        WHERE id = %s
                          AND competicao = %s
                          AND LOWER(COALESCE(status_jogo, '')) NOT IN ('finalizada', 'encerrado')
                    """, (set_seguro, partida_id, competicao))
                conn.commit()
            try:
                partida["set_atual"] = set_seguro
                partida["fase_partida"] = "papeleta"
                partida["status_jogo"] = "papeleta"
                partida["status_operacao"] = "papeleta"
            except Exception:
                pass
        except Exception as e:
            print("AVISO corrigir set atual papeleta:", repr(e), flush=True)

    return set_seguro


def _papeleta_set_atual_esta_completa_partida(partida_id, competicao, partida):
    return _papeletas_set_completas(
        partida_id=partida_id,
        competicao=competicao,
        partida=partida or {},
        verificar_fn=papeleta_set_esta_completa,
    )


# =========================================================
# PAPELETA
# =========================================================
@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def papeleta_view(competicao, partida_id):
    modo_local = request.args.get("local") == "1"
    partida = _partida_operacao_local(partida_id, competicao) if modo_local else {}
    if not partida:
        partida = buscar_partida_operacional(partida_id, competicao)
        if partida:
            _salvar_snapshot_operacao_local(partida_id, competicao, partida=partida)

    if partida and not modo_local:
        inicializar_sets_partida(partida_id, competicao)
        partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    fase = (partida.get("fase_partida") or "papeleta").strip().lower()

    if fase == "encerrado":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    # Tie-break tem prioridade sobre qualquer outro estado operacional.
    # Isso cobre tanto fase_partida='tiebreak_sorteio' quanto bancos que salvam
    # status_jogo/status_operacao='sorteio_tiebreak' ou apenas
    # tiebreak_pendente=True/tiebreak_definido=False.
    if _partida_em_sorteio_tiebreak(partida):
        flash("Antes do tie-break, faça o sorteio específico do set decisivo.", "erro")
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    if fase == "pre_jogo" and not modo_local:
        flash("Finalize primeiro o pré-jogo para acessar a papeleta.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    if fase == "jogo" and not modo_local:
        # Só manda para a tela do jogo quando a papeleta do set atual existe.
        # Entre sets, o banco pode estar como "jogo"/"em_andamento" com set_atual
        # avançado, mas sem papeleta do novo set. Nesse caso a tela correta é esta.
        _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida)
        if _papeleta_set_atual_esta_completa_partida(partida_id, competicao, partida):
            return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))

    _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida)

    if modo_local:
        equipe_a = (request.args.get("equipe_a") or partida.get("equipe_a") or "").strip()
        equipe_b = (request.args.get("equipe_b") or partida.get("equipe_b") or "").strip()
        set_atual = int(request.args.get("set") or 1)
        papeleta_a = {i: "" for i in range(1, 7)}
        papeleta_b = {i: "" for i in range(1, 7)}
    else:
        equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
            partida_id, competicao, partida
        )

    snap = _snapshot_operacao_local(partida_id, competicao)
    mapa_atletas = snap.get("atletas_por_equipe") or {}
    atletas_a = mapa_atletas.get(equipe_a) or (_listar_atletas_aprovados_cache(equipe_a, competicao) if equipe_a else [])
    atletas_b = mapa_atletas.get(equipe_b) or (_listar_atletas_aprovados_cache(equipe_b, competicao) if equipe_b else [])

    contexto = _montar_contexto_papeleta(
        competicao=competicao,
        partida=partida,
        equipe_a=equipe_a,
        equipe_b=equipe_b,
        set_atual=set_atual,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
    )
    return render_template("papeleta_apontador.html", **contexto)


@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def salvar_papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if partida:
        inicializar_sets_partida(partida_id, competicao)
        partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if str(partida.get("fase_partida") or "").strip().lower() == "encerrado":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    set_atual = _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida)
    equipe_a = partida.get("equipe_a_operacional") or partida.get("equipe_a")
    equipe_b = partida.get("equipe_b_operacional") or partida.get("equipe_b")

    atletas_cache = {}

    def atletas_equipe(equipe):
        if not equipe:
            return []
        atletas = atletas_cache.get(equipe)
        if atletas is None:
            atletas = _listar_atletas_aprovados_cache(equipe, competicao)
            atletas_cache[equipe] = atletas
        return atletas

    dados_a, rotacao_a, erros_a = _preparar_escalacao_papeleta(
        atletas=atletas_equipe(equipe_a),
        valores=_valores_formulario_papeleta(request.form, "A"),
    )
    dados_b, rotacao_b, erros_b = _preparar_escalacao_papeleta(
        atletas=atletas_equipe(equipe_b),
        valores=_valores_formulario_papeleta(request.form, "B"),
    )

    erros = erros_a + erros_b
    if erros:
        flash(erros[0], "erro")
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    salvar_papeleta(partida_id, competicao, equipe_a, set_atual, dados_a)
    salvar_papeleta(partida_id, competicao, equipe_b, set_atual, dados_b)

    # Ao salvar a papeleta de um novo set, libera oficialmente o jogo.
    # Sem isso, partidas vindas de intervalo_set/entre_sets continuavam presas
    # nessa fase no banco, mesmo após o apontador preencher a papeleta.
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE partidas
                    SET fase_partida = 'jogo',
                        status_jogo = 'em_andamento',
                        status_operacao = 'em_andamento',
                        tiebreak_pendente = FALSE
                    WHERE id = %s
                      AND competicao = %s
                """, (partida_id, competicao))
            conn.commit()
    except Exception as e:
        print("AVISO liberar jogo após papeleta:", repr(e), flush=True)


    try:
        partida_inicializada = inicializar_jogo_partida(partida_id, competicao)
        # A inicialização pode atualizar set_atual, placar de sets, saque e lados.
        # Nunca monte o cache com a linha lida antes dessa atualização.
        partida = partida_inicializada or buscar_partida_operacional(partida_id, competicao) or partida
    except Exception as e:
        print("ERRO inicializar_jogo_partida:", repr(e), flush=True)
        try:
            partida = buscar_partida_operacional(partida_id, competicao) or partida
        except Exception:
            pass

    # Reconfirma a ordem operacional após a inicialização. O placar de sets
    # continua pertencendo às equipes originais do confronto; apenas os lados
    # visuais mudam entre os sets.
    equipe_a = partida.get("equipe_a_operacional") or partida.get("equipe_a") or equipe_a
    equipe_b = partida.get("equipe_b_operacional") or partida.get("equipe_b") or equipe_b
    set_atual = int(partida.get("set_atual") or set_atual or 1)

    estado = _montar_estado_inicial_jogo(
        competicao=competicao,
        partida_id=partida_id,
        partida=partida,
        equipe_a=equipe_a,
        equipe_b=equipe_b,
        set_atual=set_atual,
        rotacao_a=rotacao_a,
        rotacao_b=rotacao_b,
    )

    # Caminho rápido: vindo da papeleta, não precisamos recalcular escudos, regras,
    # histórico ou evolução antes de redirecionar. Isso era um dos pontos que
    # travava no celular/tablet. O jogo_view usa este cache já pronto.
    _publicar_estado_apontador(
        partida_id=partida_id,
        estado=estado,
        atualizar_cache=atualizar_estado_cache,
        emitir_estado=emitir_estado_partida,
        apontador_login=_login_apontador_sessao(),
        emitir_placar=emitir_placar_apontador,
        origem="PAPELETA_RAPIDA",
    )

    _limpar_cache_apontador(competicao)
    flash("Papeleta salva com sucesso.", "sucesso")
    return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))



@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/iniciar-local", methods=["POST"])
@exigir_perfil("apontador")
def iniciar_jogo_local_view(competicao, partida_id):
    """Inicializa somente cache/socket; não grava pré-jogo nem papeleta no banco."""
    corpo = request.get_json(silent=True) or {}
    estado = dict(corpo.get("estado") or {})
    pacote = dict(corpo.get("pacote") or {})
    partida = _partida_operacao_local(partida_id, competicao)
    if not partida:
        return _json_no_cache({"ok": False, "mensagem": "Snapshot local expirou. Reabra a partida para carregar os dados."}, 409)
    estado.update({
        "ok": True, "competicao": competicao, "partida_id": partida_id,
        "fase_partida": "jogo", "status_jogo": "em_andamento",
        "local_operacao": True,
    })
    _publicar_estado_apontador(
        partida_id=partida_id,
        estado=estado,
        atualizar_cache=atualizar_estado_cache,
        emitir_estado=emitir_estado_partida,
        apontador_login=_login_apontador_sessao(),
        emitir_placar=emitir_placar_apontador,
        origem="INICIAR_JOGO_LOCAL",
    )
    _salvar_snapshot_operacao_local(partida_id, competicao, pacote_local=pacote, estado=estado)
    return _json_no_cache({
        "ok": True,
        "url": url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id, local="1"),
    })


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/heartbeat", methods=["POST"])
@exigir_perfil("apontador")
def heartbeat_partida_view(competicao, partida_id):
    login = _login_apontador_sessao()
    ok, msg = _heartbeat_partida_operador(partida_id, competicao, login)
    return _json_no_cache({"ok": ok, "mensagem": msg, "bloqueada": not ok}, 200 if ok else 423)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/liberar", methods=["POST"])
@exigir_perfil("apontador")
def liberar_partida_operacional_view(competicao, partida_id):
    login = _login_apontador_sessao()
    ok, msg = _liberar_partida_operador(partida_id, competicao, login)
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
    modo_local = request.args.get("local") == "1"
    partida = _partida_operacao_local(partida_id, competicao) if modo_local else {}
    if not partida:
        partida = buscar_partida_operacional(partida_id, competicao)
        if partida:
            _salvar_snapshot_operacao_local(partida_id, competicao, partida=partida)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    partida_lock = None
    if not modo_local:
        ok_lock, msg_lock, partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            flash(msg_lock, "erro")
            return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))
    if partida_lock:
        # ATENÇÃO:
        # validar_operador_partida() pode devolver só campos de trava
        # (id, operador, status...), não a linha completa da partida.
        # Antes a gente substituía "partida" por esse retorno parcial e perdia
        # origem/modo_operacao/sets_tipo. A consequência era o jogo do Avanço
        # abrir como scout simples mesmo quando partidas.modo_operacao estava
        # "avancado".
        try:
            partida = dict(partida or {})
            partida.update(dict(partida_lock or {}))
        except Exception:
            partida = buscar_partida_operacional(partida_id, competicao) or partida

    # Se o fim do set levou para o tie-break, a rota do jogo não pode renderizar
    # quadra nem pré-jogo: deve abrir direto o sorteio exclusivo do tie-break.
    if _partida_em_sorteio_tiebreak(partida):
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    # Garante linha completa antes de resolver scout/regras.
    # Isso cobre partidas já iniciadas, em que o cache/status de operação pode
    # ter vindo de uma consulta parcial.
    if not partida.get("modo_operacao") or not partida.get("origem") or not partida.get("equipe_a"):
        try:
            partida_completa = buscar_partida_operacional(partida_id, competicao) or {}
            if partida_completa:
                tmp = dict(partida_completa)
                tmp.update(dict(partida or {}))
                # campos completos têm prioridade para regra/scout
                for campo in ("modo_operacao", "origem", "sets_tipo", "pontos_set", "pontos_tiebreak",
                              "sets_max", "sets_para_vencer", "fase", "grupo", "equipe_a", "equipe_b",
                              "equipe_a_operacional", "equipe_b_operacional"):
                    if partida_completa.get(campo) not in (None, ""):
                        tmp[campo] = partida_completa.get(campo)
                partida = tmp
        except Exception as e:
            print("AVISO recarregar partida completa jogo_view:", repr(e), flush=True)

    modo_operacao_resolvido = _resolver_modo_operacao_partida(competicao, partida)
    try:
        partida["modo_operacao_resolvido"] = modo_operacao_resolvido
        partida["modo_operacao"] = modo_operacao_resolvido
    except Exception:
        pass

    status_jogo = (partida.get("status_jogo") or "").strip().lower()
    status_operacao = (partida.get("status_operacao") or "").strip().lower()
    fase_partida_atual = (partida.get("fase_partida") or "").strip().lower()

    editar_scout_finalizada = request.args.get("editar_scout") == "1"

    if not editar_scout_finalizada and not modo_local:
        fase_de_papeleta = {
            "papeleta",
            "entre_sets",
            "intervalo_set",
            "intervalo",
        }
        precisa_voltar_papeleta = (
            fase_partida_atual in fase_de_papeleta
            or status_jogo in fase_de_papeleta
            or status_operacao in fase_de_papeleta
        )

        if precisa_voltar_papeleta:
            _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida)
            return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

        # Proteção extra: se o set atual avançou, mas ainda não existe papeleta
        # salva para esse set, não deixa a tela do jogo reaproveitar a rotação
        # do set anterior. Isso força o preenchimento correto do 2º/3º/4º set.
        if (partida.get("equipe_a_operacional") or partida.get("equipe_a")) and (partida.get("equipe_b_operacional") or partida.get("equipe_b")):
            _corrigir_set_atual_partida_se_preciso(partida_id, competicao, partida)
            if not _papeleta_set_atual_esta_completa_partida(partida_id, competicao, partida):
                return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))
    if status_jogo in {"finalizada", "finalizado", "encerrada", "encerrado"}:
        modo_finalizada = _resolver_modo_operacao_partida(competicao, partida)
        if not (editar_scout_finalizada and modo_finalizada == "avancado"):
            flash("A partida já está finalizada.", "erro")
            return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    # Não reinicializa partida pausada. Apenas abre do ponto salvo.
    if (not editar_scout_finalizada) and (not modo_local) and status_jogo not in {"em_andamento", "entre_sets", "pausada", "pausado"} and status_operacao not in {"pausada", "pausado"}:
        try:
            partida = inicializar_jogo_partida(partida_id, competicao) or partida
        except Exception as e:
            print("ERRO inicializar_jogo_partida/jogo_view rapido:", repr(e), flush=True)

    # A inicialização pode devolver a partida com modo_operacao antigo/simples.
    # Recalcula depois dela para garantir que a regra do Avanço ganhe.
    modo_operacao_resolvido = _resolver_modo_operacao_partida(competicao, partida)
    try:
        partida["modo_operacao_resolvido"] = modo_operacao_resolvido
        partida["modo_operacao"] = modo_operacao_resolvido
        partida["permite_scout"] = modo_operacao_resolvido == "avancado"
    except Exception:
        pass

    try:
        contexto_jogo = _carregar_contexto_jogo(
            competicao=competicao,
            partida_id=partida_id,
            partida=partida,
            modo_local=modo_local,
            modo_operacao=modo_operacao_resolvido,
            obter_cache=obter_estado_cache,
            buscar_estado_banco=buscar_estado_jogo_partida,
            obter_snapshot_local=_snapshot_operacao_local,
            aplicar_escudos=_aplicar_escudos_estado,
            buscar_papeletas=_buscar_papeletas_set_atual,
            listar_atletas=_listar_atletas_aprovados_cache,
            aplicar_regras=_aplicar_regras_e_contadores_estado,
            aplicar_placar_exibicao=aplicar_placar_exibicao_partida,
            buscar_competicao=_buscar_competicao_cache,
        )
    except Exception as e:
        print("ERRO carregar contexto jogo_view:", repr(e), flush=True)
        contexto_jogo = {"ok": False, "mensagem": "Não foi possível carregar o estado da partida."}

    if not contexto_jogo.get("ok"):
        flash(contexto_jogo.get("mensagem") or "Complete o pré-jogo antes de abrir a tela do jogo.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    estado = contexto_jogo["estado"]
    papeleta_a = contexto_jogo["papeleta_a"]
    papeleta_b = contexto_jogo["papeleta_b"]
    atletas_a = contexto_jogo["atletas_a"]
    atletas_b = contexto_jogo["atletas_b"]

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
        modo_operacao=modo_operacao_resolvido,
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
        ok_lock, msg_lock, partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        dados = dict(request.get_json(silent=True) or {})
        dados.update({chave: valor for chave, valor in request.form.items() if valor is not None})

        try:
            comando, detalhes_evento = _preparar_registro_ponto(dados)
        except ErroPonto as exc:
            return _json_no_cache({"ok": False, "mensagem": str(exc)}, 400)

        ok, retorno = registrar_ponto_partida(
            partida_id=partida_id,
            competicao=competicao,
            equipe=comando["equipe_pontuadora"],
            tipo="ponto",
            detalhes=detalhes_evento,
        )
        if not ok:
            mensagem = retorno if isinstance(retorno, str) else "Não foi possível registrar o ponto."
            return _json_no_cache({"ok": False, "mensagem": mensagem}, 400)

        estado = _completar_estado_ponto(
            retorno,
            competicao=competicao,
            partida_id=partida_id,
            comando=comando,
        )

        # O ponto já foi confirmado no banco. Falha na publicação em tempo real
        # não deve devolver erro e induzir o navegador a reenviar o mesmo ponto.
        try:
            estado = _publicar_ponto(
                estado=estado,
                partida=partida_lock,
                competicao=competicao,
                partida_id=partida_id,
                obter_cache=obter_estado_cache,
                atualizar_cache=atualizar_estado_cache,
                emitir_estado=emitir_estado_partida,
                login_apontador=_login_apontador_sessao(),
                emitir_placar=emitir_placar_apontador,
            )
        except Exception as exc:
            print("AVISO emissão rápida do ponto:", repr(exc), flush=True)

        if _set_ou_jogo_finalizado(estado):
            _limpar_cache_apontador(competicao)

        url_observacoes = url_for(
            "apontadores.observacoes_view",
            competicao=competicao,
            partida_id=partida_id,
        )
        resposta = _montar_resposta_ponto(
            estado,
            competicao=competicao,
            partida_id=partida_id,
            url_observacoes=url_observacoes,
        )
        return _json_no_cache(resposta)

    except Exception as exc:
        print("ERRO ponto_view:", repr(exc), flush=True)
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar ponto: {exc}",
        }, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/wo", methods=["POST"])
@exigir_perfil("apontador")
def wo_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}

        equipe_wo = (
            request.form.get("equipe_wo")
            or corpo.get("equipe_wo")
            or ""
        ).strip().upper()
        # Compatibilidade temporária com versões antigas do HTML.
        equipe_vencedora_legado = (
            request.form.get("equipe_vencedora")
            or corpo.get("equipe_vencedora")
            or ""
        ).strip().upper()

        if equipe_wo and equipe_wo not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe que sofreu o WO é inválida."}, 400)
        if not equipe_wo and equipe_vencedora_legado not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Informe a equipe que sofreu o WO."}, 400)

        ok, retorno = registrar_wo_partida(
            partida_id=partida_id,
            competicao=competicao,
            equipe_wo=equipe_wo or None,
            vencedor_lado=equipe_vencedora_legado or None,
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
        _limpar_cache_apontador(competicao)
        _atualizar_avanco_apos_finalizacao_async(competicao)

        return _json_no_cache({
            "ok": True,
            "mensagem": estado.get("mensagem") or "Partida encerrada por WO.",
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
        _limpar_cache_apontador(competicao)

        return _json_no_cache({
            "ok": True,
            **estado
        })

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao desfazer ação: {e}"
        }, 500)


_LOCKS_ESTADO_PARTIDA = {}
_LOCKS_ESTADO_PARTIDA_GUARDA = threading.Lock()


def _lock_estado_partida(partida_id):
    chave = str(partida_id)
    with _LOCKS_ESTADO_PARTIDA_GUARDA:
        lock = _LOCKS_ESTADO_PARTIDA.get(chave)
        if lock is None:
            lock = threading.RLock()
            _LOCKS_ESTADO_PARTIDA[chave] = lock
        return lock


def _descricao_acao(tipo, equipe='', payload=None):
    return _descrever_acao_extra(tipo, equipe, payload)


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
    estado = copy.deepcopy(obter_estado_cache(partida_id) or {})

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
    if tipo in {"tempo", "retardamento", "sancao", "cartao_verde"}:
        estado = _aplicar_acao_extra_local(estado, tipo, equipe, payload)
    elif tipo in {"substituicao", "substituicao_excepcional"}:
        numero_sai = str(payload.get("numero_sai") or '').strip()
        numero_entra = str(payload.get("numero_entra") or '').strip()
        estado = _aplicar_substituicao_visual(
            estado,
            equipe=equipe,
            numero_sai=numero_sai,
            numero_entra=numero_entra,
            excepcional=(tipo == "substituicao_excepcional"),
            motivo=payload.get("motivo"),
            observacao=payload.get("observacao"),
        )

    return _normalizar_estado_pos_acao(partida_id, competicao, estado, origem=f"{tipo.upper()}_RAPIDO", acao={"descricao": descricao})


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/tempo", methods=["POST"])
@exigir_perfil("apontador")
def registrar_tempo_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        equipe_bruta = request.form.get("equipe") or corpo.get("equipe") or ""
        estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
        preparado = _preparar_tempo(equipe_bruta, estado_atual, corpo)
        estado = _acao_rapida(partida_id, competicao, "tempo", preparado["equipe"], preparado["payload"])
        return _json_no_cache({"ok": True, "local": True, "persistencia": "encerramento", "duracao": preparado["payload"]["duracao"], **estado})
    except _ErroAcaoJogo as e:
        return _json_no_cache({"ok": False, "mensagem": str(e)}, 400)
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar tempo local: {e}"}, 500)

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        equipe = str(request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()
        try:
            equipe, numero_sai, numero_entra = _validar_comando_substituicao(equipe, numero_sai, numero_entra)
        except _ErroSubstituicao as erro:
            return _json_no_cache({"ok": False, "mensagem": str(erro)}, 400)
        # A troca de rotação/status precisa ser atômica por partida.
        with _lock_estado_partida(partida_id):
            estado = _acao_rapida(
                partida_id, competicao, "substituicao", equipe,
                {**corpo, "numero_sai": numero_sai, "numero_entra": numero_entra}
            )
        return _json_no_cache({"ok": True, "local": True, "persistencia": "fim_set", **estado})
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição local: {e}"}, 500)

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao-excepcional", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_excepcional_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        equipe = str(corpo.get("equipe") or "").strip().upper()
        numero_sai = str(corpo.get("numero_sai") or "").strip()
        numero_entra = str(corpo.get("numero_entra") or "").strip()
        try:
            equipe, numero_sai, numero_entra = _validar_comando_substituicao(equipe, numero_sai, numero_entra)
        except _ErroSubstituicao as erro:
            return _json_no_cache({"ok": False, "mensagem": str(erro)}, 400)
        estado = _acao_rapida(partida_id, competicao, "substituicao_excepcional", equipe, corpo)
        return _json_no_cache({"ok": True, "local": True, "persistencia": "encerramento", **estado})
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição excepcional local: {e}"}, 500)

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/retardamento", methods=["POST"])
@exigir_perfil("apontador")
def registrar_retardamento_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        preparado = _preparar_retardamento(corpo.get("equipe"), corpo)
        estado = _acao_rapida(partida_id, competicao, "retardamento", preparado["equipe"], preparado["payload"])
        return _json_no_cache({"ok": True, "local": True, "persistencia": "encerramento", **estado})
    except _ErroAcaoJogo as e:
        return _json_no_cache({"ok": False, "mensagem": str(e)}, 400)
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar retardamento local: {e}"}, 500)

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sancao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_sancao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        preparado = _preparar_sancao(corpo.get("equipe"), corpo)
        estado = _acao_rapida(partida_id, competicao, "sancao", preparado["equipe"], preparado["payload"])
        return _json_no_cache({"ok": True, "local": True, "persistencia": "encerramento", **estado})
    except _ErroAcaoJogo as e:
        return _json_no_cache({"ok": False, "mensagem": str(e)}, 400)
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar sanção local: {e}"}, 500)

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/cartao-verde", methods=["POST"])
@exigir_perfil("apontador")
def registrar_cartao_verde_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _ = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)
        corpo = request.get_json(silent=True) or {}
        preparado = _preparar_cartao_verde(corpo.get("equipe"), corpo)
        estado = _acao_rapida(partida_id, competicao, "cartao_verde", preparado["equipe"], preparado["payload"])
        return _json_no_cache({"ok": True, "local": True, "persistencia": "encerramento", **estado})
    except _ErroAcaoJogo as e:
        return _json_no_cache({"ok": False, "mensagem": str(e)}, 400)
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar cartão verde local: {e}"}, 500)

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

        _limpar_cache_apontador(competicao)
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


def _garantir_tabela_eventos_sincronizados():
    """Compatibilidade: o schema é garantido no startup da aplicação."""
    from repositories.runtime_schema import garantir_schema_runtime
    garantir_schema_runtime()


def _ids_eventos_ja_sincronizados(partida_id, competicao, ids_locais):
    ids = [str(x).strip() for x in (ids_locais or []) if str(x).strip()]
    if not ids:
        return set()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id_local
                FROM apontador_eventos_sincronizados
                WHERE partida_id = %s AND competicao = %s AND id_local = ANY(%s)
            """, (partida_id, competicao, ids))
            return {str(row.get("id_local") or "") for row in (cur.fetchall() or [])}


def _marcar_eventos_sincronizados(partida_id, competicao, eventos):
    linhas = []
    for item in eventos or []:
        if not isinstance(item, dict):
            continue
        id_local = str(item.get("id_local") or "").strip()
        if not id_local:
            continue
        try:
            set_numero = int(item.get("set_numero") or (item.get("payload") or {}).get("set_numero") or 0) or None
        except Exception:
            set_numero = None
        linhas.append((partida_id, competicao, id_local, set_numero))
    if not linhas:
        return
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO apontador_eventos_sincronizados
                    (partida_id, competicao, id_local, set_numero)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (partida_id, competicao, id_local) DO NOTHING
            """, linhas)
        conn.commit()


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sincronizar", methods=["POST"])
@exigir_perfil("apontador")
def sincronizar_acao_view(competicao, partida_id):
    try:
        ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
        if not ok_lock:
            return _erro_operador_json(msg_lock)

        corpo = request.get_json(silent=True) or {}
        eventos_lote = corpo.get("eventos") if isinstance(corpo.get("eventos"), list) else None

        # No fim de cada set, a tela envia somente o lote daquele set em segundo
        # plano. A ida para a papeleta não aguarda esta resposta. IDs locais
        # tornam a operação idempotente em caso de queda ou reenvio.
        if eventos_lote is not None:
            ids = [str(item.get("id_local") or "").strip() for item in eventos_lote if isinstance(item, dict)]
            ja_sincronizados = _ids_eventos_ja_sincronizados(partida_id, competicao, ids)
            pendentes = [
                item for item in eventos_lote
                if isinstance(item, dict)
                and str(item.get("id_local") or "").strip()
                and str(item.get("id_local") or "").strip() not in ja_sincronizados
            ]

            confirmados = list(ja_sincronizados)
            processados = []
            confirmados_novos = []
            for item in pendentes:
                resultado = _persistir_eventos_finais_partida(partida_id, competicao, [item])
                processados.extend(resultado or [])
                if resultado and all(bool(r.get("ok")) for r in resultado if isinstance(r, dict)):
                    confirmados_novos.append(item)
                    confirmados.append(str(item.get("id_local") or "").strip())

            _marcar_eventos_sincronizados(partida_id, competicao, confirmados_novos)
            return _json_no_cache({
                "ok": True,
                "lote_sincronizado": True,
                "set_numero": corpo.get("set_numero"),
                "eventos_confirmados": confirmados,
                "quantidade_recebida": len(eventos_lote),
                "quantidade_processada": len(confirmados_novos),
                "processados": processados,
            }, 200)

        # Chamadas unitárias antigas continuam sem persistência automática.
        estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
        return _json_no_cache({
            "ok": True,
            "ignorado": True,
            "mensagem": "Ação unitária mantida localmente; lotes são sincronizados ao fim do set.",
            **estado_atual
        }, 200)

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
                "equipe_a": partida.get("equipe_a") or "",
                "equipe_b": partida.get("equipe_b") or "",
                "equipe_a_cadastro": partida.get("equipe_a") or "",
                "equipe_b_cadastro": partida.get("equipe_b") or "",
                "equipe_a_operacional": partida.get("equipe_a_operacional") or partida.get("equipe_a") or "",
                "equipe_b_operacional": partida.get("equipe_b_operacional") or partida.get("equipe_b") or "",
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
            "equipe_a": partida.get("equipe_a") or equipe_a_op,
            "equipe_b": partida.get("equipe_b") or equipe_b_op,
            "equipe_a_cadastro": partida.get("equipe_a") or equipe_a_op,
            "equipe_b_cadastro": partida.get("equipe_b") or equipe_b_op,
            "equipe_a_operacional": equipe_a_op,
            "equipe_b_operacional": equipe_b_op,
            # O jogo/apontador também precisa receber o escudo real.
            # Antes usava _escudo_payload_leve(), que substituía data:image/base64
            # por escudo padrão; por isso no celular os dois lados apareciam iguais.
            "escudo_a": _normalizar_url_escudo(estado.get("escudo_a")),
            "escudo_b": _normalizar_url_escudo(estado.get("escudo_b")),
            "escudo_a_operacional": _normalizar_url_escudo(estado.get("escudo_a_operacional") or estado.get("escudo_a")),
            "escudo_b_operacional": _normalizar_url_escudo(estado.get("escudo_b_operacional") or estado.get("escudo_b")),
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

def _persistir_pacote_operacao_local(partida_id, competicao, pacote):
    """Persiste pré-jogo, alterações de conferência e papeletas somente no encerramento."""
    pacote = pacote if isinstance(pacote, dict) else {}
    pre = pacote.get("pre_jogo") or {}
    if pre:
        ok, msg = salvar_pre_jogo_partida(
            partida_id=partida_id, competicao=competicao, operador_login=_login_apontador_sessao(),
            arbitro_1_cpf=str(pre.get("arbitro_1_cpf") or ""),
            arbitro_2_cpf=str(pre.get("arbitro_2_cpf") or ""),
            sorteio_vencedor=str(pre.get("sorteio_vencedor") or ""),
            sorteio_escolha=str(pre.get("sorteio_escolha") or ""),
            saque_inicial=str(pre.get("saque_inicial") or ""),
            lado_esquerdo=str(pre.get("lado_esquerdo") or ""),
        )
        if not ok:
            raise RuntimeError(msg or "Falha ao salvar o pré-jogo local.")

    for lado, conf in (pacote.get("conferencia") or {}).items():
        equipe = str((conf or {}).get("equipe") or "").strip()
        atletas = (conf or {}).get("atletas") or []
        if not equipe:
            continue
        libero_ids = []
        for atleta in atletas:
            atleta_id = atleta.get("id")
            if not atleta_id:
                continue
            numero = atleta.get("numero")
            atualizar_numero_atleta(atleta_id, "" if numero in (None, "") else str(numero))
            if atleta.get("libero"):
                libero_ids.append(str(atleta_id))
        ok_l, msg_l = salvar_liberos_equipe(equipe, competicao, libero_ids)
        if not ok_l:
            raise RuntimeError(msg_l or f"Falha ao salvar líberos de {equipe}.")
        marcar_equipe_conferida(competicao, equipe)

    papeletas_por_set = pacote.get("papeletas_por_set") or {str(pacote.get("set_atual") or 1): (pacote.get("papeletas") or {})}
    equipes = pacote.get("equipes_operacionais") or {}
    snap = _snapshot_operacao_local(partida_id, competicao)
    mapa = snap.get("atletas_por_equipe") or {}
    for set_chave, papeletas in papeletas_por_set.items():
        try:
            set_numero = int(set_chave)
        except Exception:
            continue
        for lado in ("A", "B"):
            equipe = str(equipes.get(lado) or "").strip()
            posicoes = (papeletas or {}).get(lado) or {}
            atletas = mapa.get(equipe) or []
            por_numero = {str(a.get("numero")): a for a in atletas if a.get("numero") not in (None, "")}
            # Considera numeração alterada localmente na conferência.
            conf = ((pacote.get("conferencia") or {}).get(lado) or {}).get("atletas") or []
            numero_local_por_id = {str(a.get("id")): a.get("numero") for a in conf if a.get("id")}
            for atleta in atletas:
                aid = str(atleta.get("id") or "")
                if aid in numero_local_por_id and numero_local_por_id[aid] not in (None, ""):
                    por_numero[str(numero_local_por_id[aid])] = atleta
            dados = {}
            for pos, numero in posicoes.items():
                atleta = por_numero.get(str(numero))
                if atleta:
                    atleta = dict(atleta)
                    atleta["numero"] = str(numero)
                    dados[int(pos)] = atleta
            if equipe and len(dados) == 6:
                salvar_papeleta(partida_id, competicao, equipe, set_numero, dados)
    return True


def _persistir_estado_final_cliente(partida_id, competicao, estado_final_cliente, pacote_operacao=None):
    """Grava explicitamente o estado final recebido do apontador antes de validar o encerramento.

    O jogo opera localmente. Por isso o banco pode estar um set atrás quando o último
    ponto é marcado. Esta função torna o payload final a fonte de verdade do fechamento,
    grava placar/sets/rotações e confirma a leitura antes de chamar encerrar_partida().
    """
    estado_cliente = dict(estado_final_cliente or {})
    pacote = pacote_operacao if isinstance(pacote_operacao, dict) else {}

    # Compatibilidade com pacotes que também carregam o estado dentro da operação local.
    estado_pacote = pacote.get("estado_final") or pacote.get("estado") or {}
    if isinstance(estado_pacote, dict):
        combinado = dict(estado_pacote)
        combinado.update(estado_cliente)
        estado_cliente = combinado

    if not estado_cliente:
        return buscar_estado_jogo_partida(partida_id, competicao) or {}

    estado_cliente, valores_extras = _preparar_estado_final_cliente(estado_cliente)

    # Persiste o snapshot vivo completo (placar, sets, rotação e disciplina).
    salvo = salvar_estado_manual_partida(
        partida_id,
        competicao,
        estado_cliente,
        operador=_login_apontador_sessao(),
        pausar=False,
    ) or {}

    # Persiste também as parciais e campos finais que não fazem parte do snapshot manual.
    if valores_extras:
        with conectar() as conn:
            with conn.cursor() as cur:
                _atualizar_partida_campos_existentes(
                    cur, partida_id, competicao, valores_extras
                )
            conn.commit()

    confirmado = buscar_estado_jogo_partida(partida_id, competicao) or salvo or {}
    _confirmar_sets_finalizacao(estado_cliente, confirmado)

    return confirmado


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/encerrar", methods=["POST"])
@exigir_perfil("apontador")
def encerrar_partida_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        pacote_operacao = corpo.get("pacote_operacao") if isinstance(corpo.get("pacote_operacao"), dict) else {}
        if not pacote_operacao:
            ok_lock, msg_lock, _partida_lock = _validar_operador_http(partida_id, competicao, renovar=True)
            if not ok_lock:
                return _erro_operador_json(msg_lock)
        else:
            # O bloqueio já foi validado ao assumir. Evita consulta extra no único envio final.
            _persistir_pacote_operacao_local(partida_id, competicao, pacote_operacao)
        observacoes = ""
        if request.is_json:
            observacoes = (corpo.get("observacoes") or "").strip()
        else:
            observacoes = (request.form.get("observacoes") or "").strip()

        eventos = corpo.get("eventos") if isinstance(corpo, dict) else []
        estado_final_cliente = corpo.get("estado_final") if isinstance(corpo.get("estado_final"), dict) else {}

        # Sets anteriores podem já ter sido sincronizados em segundo plano.
        # No encerramento processamos somente os IDs ainda pendentes.
        _eventos_iniciais, ids_eventos = _separar_eventos_finalizacao(eventos, set())
        ja_sincronizados = _ids_eventos_ja_sincronizados(partida_id, competicao, ids_eventos)
        eventos_pendentes, _ = _separar_eventos_finalizacao(eventos, ja_sincronizados)
        processados = _persistir_eventos_finais_partida(partida_id, competicao, eventos_pendentes)
        eventos_ok = _eventos_finalizacao_ok(eventos_pendentes, processados)
        _marcar_eventos_sincronizados(partida_id, competicao, eventos_ok)

        # O apontador é a fonte de verdade no modo local. Grava e confirma o
        # placar final ANTES de pedir ao banco que valide o encerramento.
        if estado_final_cliente:
            estado = _persistir_estado_final_cliente(
                partida_id, competicao, estado_final_cliente, pacote_operacao
            )
        else:
            estado = buscar_estado_jogo_partida(partida_id, competicao)
            if not estado:
                estado = dict(obter_estado_cache(partida_id) or {})

        ok_encerrar, msg_encerrar = encerrar_partida(partida_id, competicao, observacoes)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or estado or {}

        if not ok_encerrar:
            # Chamada antecipada após um set não é erro operacional. O fluxo
            # correto é voltar à papeleta para preparar o próximo set, sem
            # exibir alerta vermelho nem abrir observações/destaques.
            payload_entre_sets = _resposta_finalizacao_entre_sets(
                estado,
                url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id),
                processados,
            )
            estado = _emitir_estado_e_placar(
                partida_id, competicao, payload_entre_sets["estado"], origem="FLUXO_ENTRE_SETS"
            )
            payload_entre_sets.update(estado)
            payload_entre_sets["estado"] = estado
            _limpar_cache_apontador(competicao)
            return _json_no_cache(payload_entre_sets, 200)

        resultado_avanco = _atualizar_avanco_apos_finalizacao_async(competicao)
        if resultado_avanco:
            estado["avanco_atualizado"] = resultado_avanco
        estado = _estado_partida_finalizada(estado)
        estado["eventos_processados_final"] = processados

        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="ENCERRAR_PARTIDA_FINAL_OFFLINE")
        invalidar_cache_competicao(competicao)
        _limpar_cache_apontador(competicao)

        pendencia_destaques = verificar_destaques_competicao_pendentes(competicao, partida_id)
        payload_final = _resposta_finalizacao_concluida(
            estado,
            url_for("apontadores.observacoes_view", competicao=competicao, partida_id=partida_id),
            processados,
            pendencia_destaques,
            url_for("apontadores.destaques_competicao_view", competicao=competicao, partida_id=partida_id),
        )
        return _json_no_cache(payload_final)
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao encerrar partida: {e}"}, 500)


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def observacoes_view(competicao, partida_id):
    dados_finalizacao = listar_dados_finalizacao_partida(partida_id, competicao) or {}
    contexto = _contexto_observacoes_finalizacao(
        dados_finalizacao,
        buscar_partida_operacional(partida_id, competicao) or {},
        buscar_config_destaques_competicao(competicao) or {},
        competicao,
    )

    if not contexto["finalizada"]:
        # Proteção de rota: payload/socket antigo nunca pode abrir a finalização
        # no intervalo entre sets.
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    return render_template("observacoes.html", **contexto["template"])


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_observacoes_view(competicao, partida_id):
    """Salva toda a finalização por uma única operação transacional."""
    observacoes, destaque = _preparar_formulario_finalizacao(request.form)

    try:
        ok, mensagem, estado = finalizar_partida_completa(
            partida_id=partida_id,
            competicao=competicao,
            observacoes=observacoes,
            destaque=destaque,
            operador_login=_login_apontador_sessao(),
        )

        if not ok:
            flash(
                mensagem or "Não foi possível salvar a finalização.",
                "erro",
            )
            return redirect(
                url_for(
                    "apontadores.observacoes_view",
                    competicao=competicao,
                    partida_id=partida_id,
                )
            )

        estado = _estado_partida_finalizada(estado)

        try:
            _emitir_estado_e_placar(
                partida_id,
                competicao,
                estado,
                origem="FINALIZACAO_COMPLETA",
            )
        except Exception as e:
            print(
                "AVISO emitir estado após finalização:",
                repr(e),
                flush=True,
            )

        # O cálculo do avanço não bloqueia a resposta ao apontador.
        try:
            _atualizar_avanco_apos_finalizacao_async(competicao)
        except Exception as e:
            print(
                "AVISO agendar avanço após finalização:",
                repr(e),
                flush=True,
            )

        invalidar_cache_competicao(competicao)
        _limpar_cache_apontador(competicao)
        _operacao_local_store.remover(partida_id, competicao)

        flash(
            mensagem or "Finalização salva com sucesso.",
            "sucesso",
        )
        return redirect(
            url_for(
                "apontadores.entrar_competicao_apontador",
                competicao=competicao,
            )
        )

    except Exception as e:
        print(
            "ERRO salvar_observacoes_view/finalizacao_completa:",
            repr(e),
            flush=True,
        )
        flash(
            "Não foi possível concluir a finalização. Tente novamente.",
            "erro",
        )
        return redirect(
            url_for(
                "apontadores.observacoes_view",
                competicao=competicao,
                partida_id=partida_id,
            )
        )

@apontadores_bp.route("/apontador/destaques-competicao/<competicao>")
@exigir_perfil("apontador")
def destaques_competicao_view(competicao):
    partida_id = request.args.get("partida_id", type=int)
    cfg = buscar_config_destaques_competicao(competicao) or {}
    if not cfg.get("ativo_destaque_competicao") or not cfg.get("campos"):
        flash("O organizador ainda não ativou os destaques finais desta competição.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    pendencia = verificar_destaques_competicao_pendentes(competicao, partida_id)
    respostas = listar_respostas_destaques_competicao(competicao) or []
    respostas_por_campo = {str(r.get("campo_id") or ""): r for r in respostas}
    atletas_info = listar_atletas_para_destaques_competicao(competicao) or {}

    return render_template(
        "destaques_competicao_apontador.html",
        competicao_nome=competicao,
        partida_id=partida_id,
        config=cfg,
        pendencia=pendencia,
        respostas=respostas,
        respostas_por_campo=respostas_por_campo,
        atletas_info=atletas_info,
    )


@apontadores_bp.route("/apontador/destaques-competicao/<competicao>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_destaques_competicao_view(competicao):
    partida_id = request.form.get("partida_id", type=int)
    cfg = buscar_config_destaques_competicao(competicao) or {}
    respostas = []

    for campo in cfg.get("campos") or []:
        campo_id = str(campo.get("id") or "").strip()
        if not campo_id:
            continue
        selecionado = request.form.get(f"destaque_atleta_{campo_id}", "").strip()
        equipe = ""
        atleta_id = ""
        numero = ""
        nome = ""
        if selecionado:
            partes = selecionado.split("|||", 3)
            while len(partes) < 4:
                partes.append("")
            equipe, atleta_id, numero, nome = partes[:4]
        nome_manual = request.form.get(f"destaque_nome_{campo_id}", "").strip()
        equipe_manual = request.form.get(f"destaque_equipe_{campo_id}", "").strip()
        observacao = request.form.get(f"destaque_obs_{campo_id}", "").strip()
        if nome_manual:
            nome = nome_manual
        if equipe_manual:
            equipe = equipe_manual
        respostas.append({
            "campo_id": campo_id,
            "equipe": equipe,
            "atleta_id": atleta_id,
            "numero": numero,
            "nome": nome,
            "observacao": observacao,
        })

    ok, msg = salvar_respostas_destaques_competicao(
        competicao,
        respostas,
        preenchido_por=_login_apontador_sessao() or session.get("usuario") or "apontador",
        partida_origem_id=partida_id,
    )
    flash(msg, "sucesso" if ok else "erro")
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
        estado = _publicar_estado_apontador_sem_cache(
            partida_id=partida_id,
            estado=estado,
            emitir_estado=emitir_estado_partida,
            apontador_login=apontador_login,
            emitir_placar=emitir_placar_apontador,
            origem="INVERTER_LADOS_SEM_COMPETICAO",
        )

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

        vinculo = _buscar_vinculo_operacional_por_pin(pin_limpo)
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
