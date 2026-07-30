# Fase 2 — Sprint 46: invalidação coerente dos relatórios

## Objetivo

Evitar que a pré-visualização ou o PDF mostrem dados antigos depois da finalização de uma partida.

## Implementação

- Cada competição possui uma versão de cache própria.
- A versão passa a fazer parte da chave de todos os relatórios.
- Invalidar uma competição apenas incrementa sua versão, sem varrer chaves.
- No backend Redis, a versão é compartilhada entre Web Service e worker.
- No backend local, a versão é protegida por lock.
- A finalização da partida invalida o cache imediatamente.
- Chaves antigas permanecem somente até o TTL expirar.

## Resultado

Após salvar a partida, rankings, histórico, estatísticas e PDFs são recalculados na próxima abertura. Relatórios de outras competições não são afetados.
