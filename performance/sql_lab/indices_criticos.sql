-- Sprint 74 — candidatos iniciais para homologação.
-- Não execute em produção sem baseline, EXPLAIN e comparação posterior.
-- CREATE INDEX CONCURRENTLY não pode rodar dentro de BEGIN/COMMIT explícito.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eventos_competicao_partida
    ON eventos (competicao, partida_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_equipes_comp_nome_normalizado
    ON equipes_competicoes (competicao, LOWER(TRIM(equipe_nome)));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_equipe_a_normalizada
    ON partidas (competicao, LOWER(TRIM(equipe_a)));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_equipe_b_normalizada
    ON partidas (competicao, LOWER(TRIM(equipe_b)));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_rodada_ordem_id
    ON partidas (competicao, rodada, ordem, id);
