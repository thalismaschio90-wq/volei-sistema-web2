from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify

from banco import (
    buscar_equipe_por_login,
    buscar_partida_treinador_por_equipe,
    montar_contexto_treinador,
    salvar_papeleta,
    registrar_solicitacao_treinador,
    listar_atletas_aprovados_da_equipe,
)
from routes.utils import exigir_perfil
from socket_events import emitir_solicitacao_treinador


treinador_bp = Blueprint('treinador', __name__)


def _normalizar_rotacao_visual(rotacao):
    rotacao = rotacao or []
    if not isinstance(rotacao, list):
        return ["", "", "", "", "", ""]

    rotacao = [str(x or "").strip() for x in rotacao]

    while len(rotacao) < 6:
        rotacao.append("")

    return rotacao[:6]


def _json_erro(mensagem, status=400):
    return jsonify({'ok': False, 'mensagem': mensagem}), status


@treinador_bp.route('/treinador')
@exigir_perfil('equipe')
def abrir_modo_treinador():
    equipe = buscar_equipe_por_login(session.get('usuario'))
    if not equipe:
        flash('Equipe não encontrada.', 'erro')
        return redirect(url_for('painel.inicio'))

    partida = buscar_partida_treinador_por_equipe(equipe.get('competicao'), equipe.get('nome'))
    if not partida:
        flash('Nenhuma partida disponível para o modo treinador no momento.', 'erro')
        return redirect(url_for('equipes.minha_equipe'))

    return redirect(
        url_for(
            'treinador.tela_treinador',
            competicao=equipe.get('competicao'),
            partida_id=partida.get('id')
        )
    )


@treinador_bp.route('/treinador/jogo/<competicao>/<int:partida_id>')
@exigir_perfil('equipe')
def tela_treinador(competicao, partida_id):
    equipe = buscar_equipe_por_login(session.get('usuario'))
    if not equipe:
        flash('Equipe não encontrada.', 'erro')
        return redirect(url_for('painel.inicio'))

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get('nome'))
    if not contexto:
        flash('Partida não encontrada para esta equipe.', 'erro')
        return redirect(url_for('equipes.minha_equipe'))

    contexto['rotacao'] = _normalizar_rotacao_visual(contexto.get('rotacao'))

    return render_template('treinador_jogo.html', competicao_nome=competicao, **contexto)


@treinador_bp.route('/treinador/jogo/<competicao>/<int:partida_id>/estado')
@exigir_perfil('equipe')
def estado_treinador_view(competicao, partida_id):
    equipe = buscar_equipe_por_login(session.get('usuario'))
    if not equipe:
        return _json_erro('Equipe não encontrada.', 404)

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get('nome'))
    if not contexto:
        return _json_erro('Partida não encontrada para esta equipe.', 404)

    lado = contexto.get('lado')
    rotacao_propria = _normalizar_rotacao_visual(contexto.get('rotacao'))
    estado = contexto.get('estado') or {}

    rotacao_a = _normalizar_rotacao_visual(estado.get('rotacao_a'))
    rotacao_b = _normalizar_rotacao_visual(estado.get('rotacao_b'))

    return jsonify({
        'ok': True,
        'lado': lado,
        'placar_proprio': int(contexto.get('placar_proprio') or 0),
        'placar_adversario': int(contexto.get('placar_adversario') or 0),
        'sets_proprios': int(contexto.get('sets_proprios') or 0),
        'sets_adversario': int(contexto.get('sets_adversario') or 0),
        'saque_atual': contexto.get('saque_atual') or '',
        'tempos_restantes': contexto.get('tempos_restantes'),
        'subs_restantes': int(contexto.get('subs_restantes') or 0),
        'rotacao_propria': rotacao_propria,
        'rotacao': {
            'equipe_a': rotacao_a,
            'equipe_b': rotacao_b,
            'propria': rotacao_propria,
        },
        'solicitacoes': contexto.get('solicitacoes') or [],
    })


@treinador_bp.route('/treinador/jogo/<competicao>/<int:partida_id>/papeleta', methods=['POST'])
@exigir_perfil('equipe')
def salvar_papeleta_treinador(competicao, partida_id):
    equipe = buscar_equipe_por_login(session.get('usuario'))
    if not equipe:
        flash('Equipe não encontrada.', 'erro')
        return redirect(url_for('painel.inicio'))

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get('nome'))
    if not contexto:
        flash('Partida não encontrada para esta equipe.', 'erro')
        return redirect(url_for('equipes.minha_equipe'))

    if not contexto.get('papeleta_editavel'):
        flash('A papeleta já está travada porque o apontador iniciou o jogo.', 'erro')
        return redirect(url_for('treinador.tela_treinador', competicao=competicao, partida_id=partida_id))

    atletas = listar_atletas_aprovados_da_equipe(equipe.get('nome'), competicao) or []
    atletas_por_numero = {
        str(a.get('numero')): a
        for a in atletas
        if a.get('numero') not in (None, '')
    }

    dados = {}
    numeros_usados = set()

    for pos in [1, 2, 3, 4, 5, 6]:
        numero = (request.form.get(f'posicao_{pos}') or '').strip()

        if not numero:
            flash('Preencha as 6 posições da papeleta.', 'erro')
            return redirect(url_for('treinador.tela_treinador', competicao=competicao, partida_id=partida_id))

        if numero in numeros_usados:
            flash('Não é permitido repetir número na papeleta.', 'erro')
            return redirect(url_for('treinador.tela_treinador', competicao=competicao, partida_id=partida_id))

        atleta = atletas_por_numero.get(numero)
        if not atleta:
            flash(f'Número {numero} não encontrado entre os atletas aprovados da equipe.', 'erro')
            return redirect(url_for('treinador.tela_treinador', competicao=competicao, partida_id=partida_id))

        numeros_usados.add(numero)
        dados[pos] = atleta

    salvar_papeleta(
        partida_id,
        competicao,
        equipe.get('nome'),
        int(contexto.get('set_atual') or 1),
        dados
    )

    flash('Papeleta enviada com sucesso.', 'sucesso')
    return redirect(
        url_for(
            'treinador.tela_treinador',
            competicao=competicao,
            partida_id=partida_id,
            aba='papeleta'
        )
    )


@treinador_bp.route('/treinador/jogo/<competicao>/<int:partida_id>/solicitar-tempo', methods=['POST'])
@exigir_perfil('equipe')
def solicitar_tempo_treinador(competicao, partida_id):
    try:
        equipe = buscar_equipe_por_login(session.get('usuario'))
        if not equipe:
            return _json_erro('Equipe não encontrada.', 404)

        contexto = montar_contexto_treinador(partida_id, competicao, equipe.get('nome'))
        if not contexto:
            return _json_erro('Partida não encontrada.', 404)

        lado = contexto.get('lado')
        if not lado:
            return _json_erro('Lado da equipe não definido.', 400)

        tempos_restantes = contexto.get('tempos_restantes')
        if tempos_restantes is not None and int(tempos_restantes or 0) <= 0:
            return _json_erro('Sua equipe não tem mais tempos disponíveis.', 400)

        try:
            registrar_solicitacao_treinador(
                partida_id,
                competicao,
                lado,
                'tempo',
                {
                    'equipe_nome': equipe.get('nome'),
                    'set_atual': contexto.get('set_atual'),
                }
            )
        except Exception as e:
            print('ERRO registrar_solicitacao_treinador TEMPO:', e)

        try:
            emitir_solicitacao_treinador(partida_id, {
                'tipo': 'tempo',
                'equipe': lado,
                'equipe_nome': equipe.get('nome'),
                'mensagem': f"{equipe.get('nome')} solicitou tempo"
            })
        except Exception as e:
            print('ERRO emitir_solicitacao_treinador TEMPO:', e)

        return jsonify({
            'ok': True,
            'mensagem': 'Solicitação de tempo enviada ao apontador.'
        })

    except Exception as e:
        print('ERRO GERAL solicitar_tempo_treinador:', e)
        return _json_erro('Erro interno ao solicitar tempo.', 500)


@treinador_bp.route('/treinador/jogo/<competicao>/<int:partida_id>/solicitar-substituicao', methods=['POST'])
@exigir_perfil('equipe')
def solicitar_substituicao_treinador(competicao, partida_id):
    try:
        equipe = buscar_equipe_por_login(session.get('usuario'))
        if not equipe:
            return _json_erro('Equipe não encontrada.', 404)

        contexto = montar_contexto_treinador(partida_id, competicao, equipe.get('nome'))
        if not contexto:
            return _json_erro('Partida não encontrada.', 404)

        lado = contexto.get('lado')
        if not lado:
            return _json_erro('Lado da equipe não definido.', 400)

        try:
            registrar_solicitacao_treinador(
                partida_id,
                competicao,
                lado,
                'substituicao',
                {
                    'equipe_nome': equipe.get('nome'),
                    'set_atual': contexto.get('set_atual'),
                }
            )
        except Exception as e:
            print('ERRO registrar_solicitacao_treinador SUBSTITUICAO:', e)

        try:
            emitir_solicitacao_treinador(partida_id, {
                'tipo': 'substituicao',
                'equipe': lado,
                'equipe_nome': equipe.get('nome'),
                'mensagem': f"{equipe.get('nome')} solicitou substituição"
            })
        except Exception as e:
            print('ERRO emitir_solicitacao_treinador SUBSTITUICAO:', e)

        return jsonify({
            'ok': True,
            'mensagem': 'Solicitação de substituição enviada ao apontador.'
        })

    except Exception as e:
        print('ERRO GERAL solicitar_substituicao_treinador:', e)
        return _json_erro('Erro interno ao solicitar substituição.', 500)