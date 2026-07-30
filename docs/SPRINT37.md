# Fase 2 — Sprint 37

## Estado local recebido e proteção contra versões antigas

Esta sprint centraliza a aceitação dos estados enviados pelo navegador do apontador.

### Alterações

- novo módulo `realtime/inbound_state.py`;
- controle otimista por `estado_versao_base` e aliases legados;
- proteção por progresso esportivo para clientes que ainda não enviam versão;
- avaliação e gravação atômicas sob a mesma trava do store local;
- ACK do Socket.IO passa a informar a versão oficial salva;
- payload leve passa a transportar `estado_versao` e `estado_atualizado_em`;
- os canais `estado_partida_local` e `estado_avulso_local` usam o mesmo fluxo;
- respostas de conflito devolvem o estado oficial para reidratação da tela.

### Compatibilidade

Clientes antigos continuam protegidos pela comparação de sets, set atual e pontos. Operações explícitas de desfazer e transição de set continuam autorizadas.

### Validação

- 178 testes aprovados;
- compilação de todos os módulos Python;
- nenhum endpoint, tabela, template ou nome de evento alterado.
