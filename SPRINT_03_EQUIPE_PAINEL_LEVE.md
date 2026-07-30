# Sprint 03 — Painel da Equipe mais leve

## Objetivo
Reduzir o custo da abertura do painel inicial da equipe sem alterar regras, layout ou fluxo de navegação.

## Problema anterior
A tela inicial chamava `listar_atletas_da_equipe`, cuja consulta usa `SELECT *`. Assim, para exibir somente total, aprovados, pendentes e reprovados, o sistema carregava todos os campos de todos os atletas, inclusive fotos e documentos quando presentes.

## Alterações
- Criada `resumir_atletas_da_equipe`, com uma única consulta agregada usando `COUNT` e `FILTER`.
- O painel inicial agora recebe apenas quatro contadores, sem carregar a lista completa.
- Mantida compatibilidade do serviço `montar_resumo_painel` com listas antigas e com o novo resumo agregado.
- A lista completa de atletas continua sendo carregada normalmente nas telas que realmente precisam dela.
- Adicionado cache curto do resumo usando a mesma estrutura de invalidação dos atletas.

## Arquivos de produção
- `repositories/atletas.py`
- `services/atletas/consultas.py`
- `services/equipes/painel.py`
- `routes/equipes.py`

## Testes
- 379 testes aprovados.
- 0 falhas.

## Resultado esperado
Maior ganho em equipes que possuem muitas fotos ou documentos, pois a abertura da home deixa de transferir registros completos apenas para calcular contadores.
