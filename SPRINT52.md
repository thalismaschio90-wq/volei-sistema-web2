# Fase 2 — Sprint 52: Conferência e documentação de atletas

- Separadas consultas e escritas da conferência em `repositories/conferencia_atletas.py`.
- Criado `services/equipes/conferencia.py` para agrupamento, resumo e coordenação.
- Removidas operações `ALTER TABLE` das rotas de conferência e das consultas de leitura.
- A listagem passou a trazer, em uma consulta, os campos usados para foto, Instagram, documento, número e status.
- Mantidas fachadas legadas no `banco.py`.
- Nenhum endpoint, formulário ou template foi renomeado.
