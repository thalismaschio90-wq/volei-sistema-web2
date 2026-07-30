"""Estruturas auxiliares garantidas somente na inicialização.

Concentra DDL que antes podia ocorrer durante requisições HTTP.
"""

from threading import Lock

from repositories.conexao import conectar

_SCHEMA_LOCK = Lock()
_SCHEMA_OK = False


def garantir_schema_runtime(force: bool = False) -> None:
    global _SCHEMA_OK
    if not force:
        from core.schema_requirements import require_schema
        require_schema(
            tables=("demos_temporarias", "configuracoes_sistema", "apontador_eventos_sincronizados"),
            columns={"competicoes": ("exigir_foto_atleta", "exigir_instagram_atleta")},
            context="estruturas auxiliares de runtime",
        )
        _SCHEMA_OK = True
        return
    if _SCHEMA_OK and not force:
        return

    with _SCHEMA_LOCK:
        if _SCHEMA_OK and not force:
            return

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS demos_temporarias (
                        id SERIAL PRIMARY KEY,
                        codigo TEXT UNIQUE NOT NULL,
                        nome TEXT NOT NULL DEFAULT '',
                        cpf TEXT NOT NULL DEFAULT '',
                        whatsapp TEXT NOT NULL DEFAULT '',
                        competicao TEXT UNIQUE NOT NULL,
                        login TEXT UNIQUE NOT NULL,
                        senha TEXT NOT NULL,
                        criado_em TIMESTAMP DEFAULT NOW(),
                        expira_em TIMESTAMP NOT NULL,
                        encerrada BOOLEAN DEFAULT FALSE,
                        motivo_encerramento TEXT DEFAULT '',
                        whatsapp_enviado BOOLEAN DEFAULT FALSE,
                        liberado_novo_teste BOOLEAN DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    ALTER TABLE demos_temporarias
                    ADD COLUMN IF NOT EXISTS nome TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS cpf TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS whatsapp TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS motivo_encerramento TEXT DEFAULT '',
                    ADD COLUMN IF NOT EXISTS whatsapp_enviado BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS liberado_novo_teste BOOLEAN DEFAULT FALSE
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS configuracoes_sistema (
                        chave TEXT PRIMARY KEY,
                        valor TEXT NOT NULL DEFAULT '',
                        atualizado_em TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS apontador_eventos_sincronizados (
                        partida_id INTEGER NOT NULL,
                        competicao TEXT NOT NULL,
                        id_local TEXT NOT NULL,
                        set_numero INTEGER,
                        sincronizado_em TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (partida_id, competicao, id_local)
                    )
                """)
                cur.execute("""
                    ALTER TABLE competicoes
                    ADD COLUMN IF NOT EXISTS exigir_foto_atleta BOOLEAN DEFAULT FALSE
                """)
                cur.execute("""
                    ALTER TABLE competicoes
                    ADD COLUMN IF NOT EXISTS exigir_instagram_atleta BOOLEAN DEFAULT FALSE
                """)
            conn.commit()
        _SCHEMA_OK = True
