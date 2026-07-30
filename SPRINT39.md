# Fase 2 — Sprint 39

## Controle de versão nos árbitros e no placar profissional

As telas `arbitro_unico.html`, `primeiro_arbitro.html`, `segundo_arbitro.html` e `placar_profissional.html` agora:

- armazenam a maior `estado_versao` aplicada;
- reconhecem a versão no payload principal ou em `estado` aninhado;
- descartam atualizações Socket.IO mais antigas;
- mantêm compatibilidade com payloads legados sem versão;
- permitem regressões explicitamente autorizadas, como desfazer ponto e transição de set.

Nenhum endpoint, evento Socket.IO, sala ou tabela foi alterado.
