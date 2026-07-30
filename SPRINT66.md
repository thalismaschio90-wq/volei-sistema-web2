# Sprint 66 — Renderização seletiva por chaves alteradas

Foi criado um planejador compartilhado de renderização para classificar as chaves recebidas por delta em grupos como placar, sets, saque, equipes, rotação, disciplina, timeline e destaque.

O placar profissional agora altera somente os blocos do DOM relacionados às chaves modificadas. O visualizador público deixou de disparar a consulta completa de detalhes quando o delta contém apenas alterações operacionais que não afetam placar, timeline, scout ou destaque.

As demais telas críticas já carregam o planejador e ficam preparadas para adoção progressiva da renderização parcial sem alterar o protocolo Socket.IO.
