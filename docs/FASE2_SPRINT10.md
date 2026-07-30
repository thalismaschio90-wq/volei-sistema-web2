# Fase 2 — Sprint 10

Migração do ciclo básico de competições para módulos próprios.

Migrado nesta etapa:
- sincronização de status;
- listagens gerais e por organizador;
- busca por organizador;
- verificação de existência;
- criação com credenciais do organizador;
- consulta, trava e destrava da competição.

A exclusão completa permanece no `banco.py` por ser uma rotina extensa e sensível,
e será migrada isoladamente em uma sprint própria com testes de integridade.
