# VolleyTablePro — Fase 2 / Sprint 23

## Objetivo
Separar de `routes/tabela.py` a montagem do contexto das abas Configurações, Partidas, Classificação e Visualizador.

## Alterações
- criado `services/competicoes/tabela_contexto.py`;
- normalização de aba e fase centralizada;
- contexto-base do template centralizado;
- carregamento específico de cada aba movido para o serviço;
- rota preserva sessão, parâmetros HTTP, cache, URLs e renderização;
- partidas continuam fora do pacote cacheado para não congelar placar ao vivo;
- formatos de contexto, endpoints e templates preservados.

## Resultado estrutural
- `routes/tabela.py`: aproximadamente 2.445 → 2.358 linhas;
- novo serviço: 235 linhas;
- nenhuma tabela do banco alterada;
- nenhum endpoint alterado;
- nenhum template alterado.

## Validação
- 95 testes aprovados;
- compilação de todos os módulos Python concluída;
- permanece somente o aviso legado de `\\d` em `routes/competicoes.py`.

## Homologação recomendada
Abrir as quatro abas da tabela e validar:
1. Configurações: grupos, equipes, quadras e agenda;
2. Partidas: placar ao vivo, fases e séries;
3. Classificação: critérios e colunas;
4. Visualizador: link curto e fallback público.
