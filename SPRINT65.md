# Fase 3 — Sprint 65

## Renderização agrupada dos deltas

Esta sprint reduz o custo de atualização do DOM nas telas em tempo real.

### Alterações

Foi criado `static/js/realtime/render_scheduler.js`, que:

- agrupa deltas recebidos no mesmo quadro de renderização;
- usa somente o estado mais recente;
- acumula as chaves alteradas;
- executa no máximo uma renderização por `requestAnimationFrame`;
- possui fallback de 16 ms para navegadores sem essa API.

### Telas adaptadas

- jogo do apontador;
- primeiro árbitro;
- segundo árbitro;
- árbitro único;
- placar profissional;
- visualizador público da partida.

### Compatibilidade

O estado continua sendo aplicado pelas funções existentes de cada tela. Não houve alteração em endpoints, eventos Socket.IO, banco, regras ou layout.

### Validação

- 267 testes Python aprovados;
- teste Node do agrupamento de renderizações aprovado;
- validação sintática dos arquivos JavaScript compartilhados.
