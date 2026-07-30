(function (global) {
  'use strict';

  function agora() {
    if (global.performance && typeof global.performance.now === 'function') {
      return global.performance.now();
    }
    return Date.now();
  }

  function criarAgendador(opcoes) {
    opcoes = opcoes || {};
    var pendente = false;
    var ultimoEstado = null;
    var metadadosAcumulados = {
      chaves: {},
      removidas: {},
      quantidade: 0,
      primeiraEm: 0,
      ultimaEm: 0
    };

    var raf = typeof global.requestAnimationFrame === 'function'
      ? global.requestAnimationFrame.bind(global)
      : function (callback) { return setTimeout(callback, 16); };

    var metricasPendentes = [];
    var metricasTimer = null;

    function descarregarMetricas() {
      if (metricasTimer) { clearTimeout(metricasTimer); metricasTimer = null; }
      if (!metricasPendentes.length || !opcoes.telemetryClientType) return;
      var lote = metricasPendentes.slice(0, 20);
      metricasPendentes = metricasPendentes.slice(20);
      try {
        fetch(String(opcoes.telemetryEndpoint || '/realtime/delta-telemetria'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            tipo_cliente: String(opcoes.telemetryClientType),
            renderizacoes: lote
          }),
          keepalive: true,
          credentials: 'same-origin'
        }).catch(function () {});
      } catch (_) {}
      if (metricasPendentes.length) metricasTimer = setTimeout(descarregarMetricas, 1000);
    }

    function registrarMetrica(duracao, meta) {
      if (!opcoes.telemetryClientType) return;
      metricasPendentes.push({
        duracao_ms: Math.max(0, Number(duracao || 0)),
        quantidade_agregada: Math.max(1, Number(meta.quantidade_agregada || 1))
      });
      if (metricasPendentes.length >= 10) { descarregarMetricas(); return; }
      if (!metricasTimer) metricasTimer = setTimeout(descarregarMetricas, 5000);
    }

    function limparMetadados() {
      metadadosAcumulados = {
        chaves: {},
        removidas: {},
        quantidade: 0,
        primeiraEm: 0,
        ultimaEm: 0
      };
    }

    function acumularMetadados(meta) {
      meta = meta || {};
      var instante = agora();
      if (!metadadosAcumulados.primeiraEm) metadadosAcumulados.primeiraEm = instante;
      metadadosAcumulados.ultimaEm = instante;
      metadadosAcumulados.quantidade += 1;
      (meta.chaves || []).forEach(function (chave) {
        metadadosAcumulados.chaves[String(chave)] = true;
      });
      (meta.removidas || []).forEach(function (chave) {
        metadadosAcumulados.removidas[String(chave)] = true;
      });
    }

    function executar() {
      pendente = false;
      if (!ultimoEstado) return;

      var estado = ultimoEstado;
      ultimoEstado = null;
      var meta = {
        chaves: Object.keys(metadadosAcumulados.chaves),
        removidas: Object.keys(metadadosAcumulados.removidas),
        quantidade_agregada: metadadosAcumulados.quantidade,
        espera_ms: Math.max(0, metadadosAcumulados.ultimaEm - metadadosAcumulados.primeiraEm)
      };
      limparMetadados();

      var inicio = agora();
      try {
        if (typeof opcoes.render === 'function') opcoes.render(estado, meta);
      } finally {
        var duracao = Math.max(0, agora() - inicio);
        registrarMetrica(duracao, meta);
        if (typeof opcoes.onRendered === 'function') {
          opcoes.onRendered({
            duracao_ms: duracao,
            quantidade_agregada: meta.quantidade_agregada,
            chaves: meta.chaves,
            removidas: meta.removidas
          });
        }
      }
    }

    function agendar(estado, meta) {
      ultimoEstado = estado;
      acumularMetadados(meta);
      if (pendente) return;
      pendente = true;
      raf(executar);
    }

    return {
      schedule: agendar,
      flush: executar,
      isPending: function () { return pendente; }
    };
  }

  global.VTPRealtimeRenderScheduler = {
    create: criarAgendador
  };
})(window);
