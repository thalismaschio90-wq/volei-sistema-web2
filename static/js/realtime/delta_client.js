(function (global) {
  'use strict';

  function numeroVersao(valor) {
    var numero = Number(valor || 0);
    return Number.isFinite(numero) && numero > 0 ? Math.trunc(numero) : 0;
  }

  function clonar(valor) {
    if (valor == null || typeof valor !== 'object') return valor;
    if (typeof structuredClone === 'function') {
      try { return structuredClone(valor); } catch (_) {}
    }
    try { return JSON.parse(JSON.stringify(valor)); } catch (_) {}
    if (Array.isArray(valor)) return valor.slice();
    return Object.assign({}, valor);
  }

  function enviarTelemetriaLote(opcoes, eventos) {
    if (opcoes.telemetry === false || !eventos || !eventos.length) return;
    var endpoint = String(opcoes.telemetryEndpoint || '/realtime/delta-telemetria');
    var corpo = {
      tipo_cliente: String(opcoes.clientType || 'desconhecido'),
      eventos: eventos
    };
    try {
      fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(corpo),
        keepalive: true,
        credentials: 'same-origin'
      }).catch(function () {});
    } catch (_) {}
  }

  function aplicarPatchDicionario(destino, patch) {
    var resultado = (destino && typeof destino === 'object' && !Array.isArray(destino))
      ? clonar(destino)
      : {};

    Object.keys(patch || {}).forEach(function (chave) {
      var valor = patch[chave];
      if (
        valor && typeof valor === 'object' && !Array.isArray(valor) &&
        Object.prototype.hasOwnProperty.call(valor, '__vtp_patch__') &&
        Object.prototype.hasOwnProperty.call(valor, '__vtp_removidas__')
      ) {
        var base = resultado[chave];
        var atualizado = aplicarPatchDicionario(base, valor.__vtp_patch__ || {});
        (valor.__vtp_removidas__ || []).forEach(function (removida) {
          delete atualizado[String(removida)];
        });
        resultado[chave] = atualizado;
      } else {
        resultado[chave] = clonar(valor);
      }
    });
    return resultado;
  }

  function aplicarDelta(estado, delta) {
    var atualizado = aplicarPatchDicionario(estado || {}, delta.patch || {});
    (delta.chaves_removidas || []).forEach(function (chave) {
      delete atualizado[String(chave)];
    });
    var versao = numeroVersao(delta.estado_versao);
    if (versao) atualizado.estado_versao = versao;
    return atualizado;
  }

  function criarCliente(opcoes) {
    opcoes = opcoes || {};
    var aguardandoSnapshot = false;
    var ultimaSolicitacaoSnapshotEm = 0;
    var telemetriaPendente = {};
    var telemetriaTimer = null;

    function descarregarTelemetria() {
      if (telemetriaTimer) { clearTimeout(telemetriaTimer); telemetriaTimer = null; }
      var eventos = Object.keys(telemetriaPendente).map(function (evento) {
        return { evento: evento, quantidade: telemetriaPendente[evento] };
      });
      telemetriaPendente = {};
      enviarTelemetriaLote(opcoes, eventos);
    }

    function registrarTelemetria(evento, quantidade, imediato) {
      if (opcoes.telemetry === false || !evento) return;
      telemetriaPendente[evento] = (telemetriaPendente[evento] || 0) + Math.max(1, Number(quantidade || 1));
      var total = Object.keys(telemetriaPendente).reduce(function (soma, chave) { return soma + telemetriaPendente[chave]; }, 0);
      if (imediato || total >= 10) { descarregarTelemetria(); return; }
      if (!telemetriaTimer) telemetriaTimer = setTimeout(descarregarTelemetria, 5000);
    }

    function estadoAtual() {
      var estado = typeof opcoes.getState === 'function' ? opcoes.getState() : {};
      return estado && typeof estado === 'object' ? estado : {};
    }

    function versaoAtual() {
      if (typeof opcoes.getVersion === 'function') return numeroVersao(opcoes.getVersion());
      return numeroVersao(estadoAtual().estado_versao);
    }

    function definirEstado(estado, versao, metadados) {
      if (typeof opcoes.setState === 'function') opcoes.setState(estado, metadados || {});
      if (typeof opcoes.setVersion === 'function') opcoes.setVersion(numeroVersao(versao));
    }

    function solicitarSnapshot(motivo, delta) {
      var agora = Date.now();
      if (aguardandoSnapshot && (agora - ultimaSolicitacaoSnapshotEm) < 1500) return;
      aguardandoSnapshot = true;
      ultimaSolicitacaoSnapshotEm = agora;
      var contexto = {
        motivo: motivo,
        versao_atual: versaoAtual(),
        versao_base: numeroVersao(delta && delta.estado_versao_base),
        versao_recebida: numeroVersao(delta && delta.estado_versao),
        delta: delta || null
      };
      if (typeof opcoes.onRecoveryRequired === 'function') {
        var recuperacaoSolicitada = opcoes.onRecoveryRequired(contexto);
        if (recuperacaoSolicitada !== false) return;
      }
      if (typeof opcoes.onSnapshotRequired === 'function') opcoes.onSnapshotRequired(contexto);
    }

    function aceitarSnapshot(snapshot) {
      if (!snapshot || typeof snapshot !== 'object') return false;
      var versao = numeroVersao(snapshot.estado_versao || snapshot.versao_estado);
      definirEstado(clonar(snapshot), versao, { tipo: 'snapshot' });
      aguardandoSnapshot = false;
      registrarTelemetria('snapshot_aceito', 1, true);
      return true;
    }

    function receber(delta) {
      if (!delta || typeof delta !== 'object' || !delta.payload_delta) {
        return { aplicado: false, motivo: 'nao_delta' };
      }

      var partidaEsperada = String(opcoes.partidaId == null ? '' : opcoes.partidaId);
      var partidaRecebida = String(delta.partida_id == null ? '' : delta.partida_id);
      if (partidaEsperada && partidaRecebida && partidaEsperada !== partidaRecebida) {
        registrarTelemetria('outra_partida', 1, false);
        return { aplicado: false, motivo: 'outra_partida' };
      }

      var atual = versaoAtual();
      var base = numeroVersao(delta.estado_versao_base);
      var recebida = numeroVersao(delta.estado_versao);

      if (!recebida) {
        registrarTelemetria('delta_invalido', 1, false);
        return { aplicado: false, motivo: 'delta_invalido', versao: atual };
      }
      if (recebida <= atual) {
        registrarTelemetria('delta_antigo', 1, false);
        return { aplicado: false, motivo: 'duplicado_ou_antigo', versao: atual };
      }

      if (!atual || base !== atual) {
        registrarTelemetria('lacuna_versao', 1, true);
        registrarTelemetria('snapshot_solicitado', 1, true);
        solicitarSnapshot('lacuna_de_versao', delta);
        return { aplicado: false, snapshot_necessario: true, motivo: 'lacuna_de_versao', versao: atual };
      }

      var atualizado = aplicarDelta(estadoAtual(), delta);
      definirEstado(atualizado, recebida, {
        tipo: 'delta',
        chaves: Object.keys(delta.patch || {}),
        removidas: (delta.chaves_removidas || []).slice()
      });
      aguardandoSnapshot = false;
      registrarTelemetria('delta_aplicado', 1, false);
      if (typeof opcoes.onApplied === 'function') {
        opcoes.onApplied(atualizado, delta);
      }
      return { aplicado: true, estado: atualizado, versao: recebida };
    }

    function receberRecuperacao(payload) {
      if (!payload || typeof payload !== 'object' || payload.ok === false) {
        aguardandoSnapshot = false;
        if (typeof opcoes.onSnapshotRequired === 'function') opcoes.onSnapshotRequired({ motivo: 'recuperacao_indisponivel' });
        return { aplicado: false, motivo: 'recuperacao_indisponivel' };
      }
      var modo = String(payload.modo || '').toLowerCase();
      if (modo === 'atualizado') {
        aguardandoSnapshot = false;
        return { aplicado: true, modo: modo, versao: versaoAtual() };
      }
      if (modo === 'snapshot' && payload.snapshot) {
        return { aplicado: aceitarSnapshot(payload.snapshot), modo: modo, versao: versaoAtual() };
      }
      if (modo === 'eventos' && Array.isArray(payload.eventos)) {
        var aplicados = 0;
        for (var i = 0; i < payload.eventos.length; i += 1) {
          var resultado = receber(payload.eventos[i]);
          if (!resultado.aplicado) {
            aguardandoSnapshot = false;
            if (typeof opcoes.onSnapshotRequired === 'function') opcoes.onSnapshotRequired({ motivo: 'falha_na_recuperacao', payload: payload });
            return { aplicado: false, motivo: 'falha_na_recuperacao', aplicados: aplicados };
          }
          aplicados += 1;
        }
        aguardandoSnapshot = false;
        registrarTelemetria('recuperacao_eventos', Math.max(1, aplicados), true);
        return { aplicado: true, modo: modo, aplicados: aplicados, versao: versaoAtual() };
      }
      aguardandoSnapshot = false;
      if (typeof opcoes.onSnapshotRequired === 'function') opcoes.onSnapshotRequired({ motivo: 'modo_recuperacao_invalido', payload: payload });
      return { aplicado: false, motivo: 'modo_recuperacao_invalido' };
    }

    return {
      receber: receber,
      receberRecuperacao: receberRecuperacao,
      aceitarSnapshot: aceitarSnapshot,
      aplicarDelta: aplicarDelta,
      versaoAtual: versaoAtual,
      estaAguardandoSnapshot: function () { return aguardandoSnapshot; }
    };
  }

  global.VTPRealtimeDelta = {
    create: criarCliente,
    apply: aplicarDelta,
    version: numeroVersao
  };
})(window);
