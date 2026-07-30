# Fase 2 — Sprint 2: consultas de equipes e atletas

## Objetivo

Iniciar a migração real por domínio, retirando consultas de alta frequência do
arquivo `banco.py` sem alterar os nomes públicos usados pelo restante do sistema.

## Arquivos novos

- `repositories/equipes.py`
- `repositories/atletas.py`
- `services/equipes/consultas.py`
- `services/atletas/consultas.py`

## Consultas migradas

### Equipes

- `listar_equipes_da_competicao`
- `buscar_equipe_por_nome_e_competicao`
- `buscar_equipe_por_login`

### Atletas

- `listar_atletas_da_equipe`
- `listar_atletas_aprovados_da_equipe`
- `contar_atletas_da_equipe`
- `numero_atleta_disponivel`

## Compatibilidade

O final de `banco.py` mantém aliases para as implementações novas. Assim,
integrações antigas continuam funcionando enquanto as rotas são migradas aos
poucos.

## Melhoria de desempenho

As consultas migradas não executam mais comandos de criação/alteração de schema
em cada leitura. As garantias de schema relacionadas às equipes foram colocadas
na inicialização da aplicação. Isso reduz idas desnecessárias ao PostgreSQL nas
páginas de equipe, treinador, tabela, apontador e no cabeçalho global.

## Próxima etapa

Migrar as operações de escrita de equipes e atletas, com serviços para regras de
cadastro, validação e permissões. Depois disso, iniciar o domínio de competições.
