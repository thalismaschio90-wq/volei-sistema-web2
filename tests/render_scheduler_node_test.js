const fs = require('fs');
const vm = require('vm');
const path = require('path');

const callbacks = [];
const context = {
  window: {
    requestAnimationFrame: (cb) => { callbacks.push(cb); return callbacks.length; },
    performance: { now: (() => { let n = 0; return () => ++n; })() }
  },
  setTimeout,
  clearTimeout,
  Date,
  console
};
context.window.window = context.window;
vm.createContext(context);
const source = fs.readFileSync(path.join(__dirname, '../static/js/realtime/render_scheduler.js'), 'utf8');
vm.runInContext(source, context);

let renders = 0;
let ultimo = null;
let metaFinal = null;
const scheduler = context.window.VTPRealtimeRenderScheduler.create({
  render: (estado, meta) => { renders += 1; ultimo = estado; metaFinal = meta; }
});

scheduler.schedule({ pontos_a: 1 }, { chaves: ['pontos_a'] });
scheduler.schedule({ pontos_a: 2, saque_atual: 'A' }, { chaves: ['pontos_a', 'saque_atual'] });
if (renders !== 0) throw new Error('render executou antes do quadro');
if (callbacks.length !== 1) throw new Error('mais de um quadro foi agendado');
callbacks.shift()();
if (renders !== 1) throw new Error('atualizacoes nao foram agregadas');
if (ultimo.pontos_a !== 2) throw new Error('estado mais recente nao foi usado');
if (metaFinal.quantidade_agregada !== 2) throw new Error('quantidade agregada incorreta');
if (!metaFinal.chaves.includes('saque_atual')) throw new Error('chaves alteradas nao foram acumuladas');
