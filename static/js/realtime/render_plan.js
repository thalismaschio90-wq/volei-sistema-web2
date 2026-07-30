(function (global) {
  'use strict';

  var GRUPOS = {
    placar: ['pontos_a', 'pontos_b', 'placar_a', 'placar_b', 'equipe_pontuadora', 'tipo_evento'],
    sets: ['sets_a', 'sets_b', 'set_atual', 'fim_set', 'set_finalizado', 'partida_finalizada'],
    saque: ['saque_atual', 'saque'],
    equipes: [
      'equipe_a', 'equipe_b', 'equipe_a_operacional', 'equipe_b_operacional',
      'nome_equipe_a', 'nome_equipe_b', 'escudo_a', 'escudo_b',
      'escudo_a_operacional', 'escudo_b_operacional', 'cor_a', 'cor_b',
      'cor_a_operacional', 'cor_b_operacional', 'lado_invertido', 'invertido'
    ],
    timeline: [
      'evolucao_pontos', 'historico', 'ultima_acao', 'ultimo_evento',
      'versao_eventos', 'eventos_versao', 'scout', 'estatisticas'
    ],
    destaque: ['destaque', 'destaque_partida', 'versao_destaque'],
    disciplina: [
      'sancoes_a', 'sancoes_b', 'cartoes_vermelhos_a', 'cartoes_vermelhos_b',
      'cartoes_verdes_a', 'cartoes_verdes_b', 'retardamentos_a', 'retardamentos_b',
      'tempos_a', 'tempos_b', 'tempo_ativo'
    ],
    rotacao: ['rotacao_a', 'rotacao_b', 'status_jogadores_a', 'status_jogadores_b', 'subs_a', 'subs_b'],
    status: ['status', 'status_jogo', 'status_operacao', 'encerrado', 'fim_jogo', 'competicao']
  };

  function normalizar(lista) {
    var mapa = {};
    (lista || []).forEach(function (item) {
      if (item === undefined || item === null) return;
      mapa[String(item)] = true;
    });
    return mapa;
  }

  function grupoAlterado(mapa, nome) {
    return (GRUPOS[nome] || []).some(function (chave) { return !!mapa[chave]; });
  }

  function planejar(chaves, removidas) {
    var mapa = normalizar((chaves || []).concat(removidas || []));
    var possuiChaves = Object.keys(mapa).length > 0;
    var plano = { completo: !possuiChaves, chaves: Object.keys(mapa) };

    Object.keys(GRUPOS).forEach(function (nome) {
      plano[nome] = !possuiChaves || grupoAlterado(mapa, nome);
    });

    plano.topo = plano.completo || plano.placar || plano.sets || plano.saque || plano.equipes || plano.status;
    plano.detalhes = plano.completo || plano.timeline || plano.destaque || plano.placar || plano.sets;
    plano.quadra = plano.completo || plano.rotacao || plano.equipes || plano.saque;
    plano.painel = plano.completo || plano.disciplina || plano.status || plano.placar || plano.sets;
    return plano;
  }

  global.VTPRealtimeRenderPlan = {
    groups: GRUPOS,
    plan: planejar
  };
})(window);
