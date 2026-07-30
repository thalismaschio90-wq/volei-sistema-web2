# Sprint 09 — Rotas de equipe usando serviços de domínio

## Arquivos de produção alterados

- `routes/equipes.py`
- `services/equipes/perfil.py`

## Alterações

- A rota de equipes deixou de importar diretamente de `banco.py` as operações de:
  - criação de nova equipe;
  - vínculo de equipe existente;
  - salvamento do perfil;
  - verificação de perfil incompleto;
  - renomeação da equipe;
  - atualização do escudo.
- Essas operações agora são obtidas dos módulos `services.equipes.cadastro` e `services.equipes.perfil`.
- A assinatura antiga de `salvar_perfil_equipe_por_login` foi preservada no serviço para manter compatibilidade com os formulários e rotas existentes.
- Regras e respostas das funções não foram alteradas.

## Validação

- Compilação Python: aprovada.
- Testes direcionados da área de equipes: 29 aprovados.
- Suíte completa: 371 aprovados e 2 falhas preexistentes fora desta sprint:
  - teste do registro de handlers em `socket-sync.js`;
  - teste antigo do painel do organizador incompatível com a consulta consolidada atual.
