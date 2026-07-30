(function (global) {
  'use strict';

  function numero(valor, padrao) {
    const n = Number(valor);
    return Number.isFinite(n) ? n : padrao;
  }

  class RealtimeConnectionGuard {
    constructor(opcoes) {
      const cfg = opcoes || {};
      this.minDelay = Math.max(250, numero(cfg.minDelay, 1500));
      this.maxDelay = Math.max(this.minDelay, numero(cfg.maxDelay, 30000));
      this.connectedDelay = Math.max(this.minDelay, numero(cfg.connectedDelay, 10000));
      this.hiddenDelay = Math.max(this.connectedDelay, numero(cfg.hiddenDelay, 30000));
      this.finishedDelay = Math.max(this.connectedDelay, numero(cfg.finishedDelay, 20000));
      this.jitter = Math.min(0.75, Math.max(0, numero(cfg.jitter, 0.25)));
      this.failures = 0;
      this.connected = false;
      this.lastSuccessAt = 0;
      this.lastFailureAt = 0;
    }

    registrarConexao() {
      this.connected = true;
      this.failures = 0;
      this.lastSuccessAt = Date.now();
    }

    registrarDesconexao() {
      this.connected = false;
    }

    registrarSucesso() {
      this.failures = 0;
      this.lastSuccessAt = Date.now();
    }

    registrarFalha() {
      this.failures = Math.min(8, this.failures + 1);
      this.lastFailureAt = Date.now();
    }

    _comJitter(valor) {
      if (!this.jitter) return Math.round(valor);
      const faixa = valor * this.jitter;
      return Math.max(250, Math.round(valor - faixa + (Math.random() * faixa * 2)));
    }

    proximoAtraso(contexto) {
      const ctx = contexto || {};
      const oculto = ctx.oculto !== undefined ? !!ctx.oculto : !!(global.document && global.document.hidden);
      const conectado = ctx.conectado !== undefined ? !!ctx.conectado : this.connected;
      const finalizada = !!ctx.finalizada;
      const aoVivo = ctx.aoVivo !== false;

      let base;
      if (oculto) {
        base = this.hiddenDelay;
      } else if (finalizada) {
        base = this.finishedDelay;
      } else if (conectado) {
        base = this.connectedDelay;
      } else if (!aoVivo) {
        base = Math.max(this.connectedDelay, 8000);
      } else {
        base = this.minDelay * Math.pow(2, this.failures);
      }

      return this._comJitter(Math.min(this.maxDelay, Math.max(this.minDelay, base)));
    }
  }

  global.VTPRealtimeConnectionGuard = {
    criar: function (opcoes) {
      return new RealtimeConnectionGuard(opcoes);
    },
    RealtimeConnectionGuard: RealtimeConnectionGuard
  };
})(window);
