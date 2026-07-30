"""Executor versionado das migrações de schema do VolleyTablePro.

As migrações são executadas antes da inicialização do Gunicorn, nunca durante o
import de ``app.py``. Um advisory lock do PostgreSQL serializa deploys
concorrentes e a tabela ``vtp_schema_migrations`` impede repetição de etapas já
concluídas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import importlib
import os
import socket
from typing import Callable, Iterable

from core.schema_ddl_guard import permitir_ddl_migracao


_LOCK_NAMESPACE = "volleytablepro:schema-migrations:v1"
_LOCK_ID = int.from_bytes(hashlib.sha256(_LOCK_NAMESPACE.encode("utf-8")).digest()[:8], "big", signed=True)


@dataclass(frozen=True)
class MigrationStep:
    version: str
    description: str
    runner_path: str
    force_keyword: bool = True

    def resolve_runner(self) -> Callable[[], None]:
        module_name, function_name = self.runner_path.split(":", 1)
        function = getattr(importlib.import_module(module_name), function_name)
        if self.force_keyword:
            return lambda: function(force=True)
        return function


def _steps() -> tuple[MigrationStep, ...]:
    # Caminhos textuais permitem listar as migrações sem importar psycopg ou o
    # módulo legado ``banco.py`` no modo dry-run.
    return (
        MigrationStep("2026_07_28_001", "estruturas auxiliares de runtime", "repositories.runtime_schema:garantir_schema_runtime"),
        MigrationStep("2026_07_28_002", "estrutura de rotação profissional", "banco:criar_estrutura_rotacao_profissional"),
        MigrationStep("2026_07_28_003", "atalhos do apontador", "banco:criar_tabela_atalhos_apontador"),
        MigrationStep("2026_07_28_004", "vínculos de equipes e competições", "banco:criar_tabela_equipes_competicoes"),
        MigrationStep("2026_07_28_005", "campos de perfil das equipes", "banco:criar_campos_perfil_equipe"),
        MigrationStep("2026_07_28_006", "campo de escudo das equipes", "banco:criar_campo_escudo_equipes"),
        MigrationStep("2026_07_28_007", "campos do quadro técnico", "banco:criar_campos_quadro_tecnico_equipes"),
        MigrationStep("2026_07_28_008", "campos de liberação extraordinária", "banco:criar_campos_liberacao_extra_equipes"),
        MigrationStep("2026_07_28_009", "trava operacional das partidas", "banco:garantir_campos_trava_operacional_partida"),
        MigrationStep("2026_07_28_010", "cache de classificação", "banco:criar_tabela_cache_classificacao"),
        MigrationStep("2026_07_28_011", "código público das competições", "banco:garantir_schema_codigo_publico_competicoes"),
        MigrationStep("2026_07_28_012", "cadastro de atletas", "banco:criar_tabela_atletas"),
        MigrationStep("2026_07_28_013", "tabelas de oficiais", "banco:criar_tabelas_oficiais"),
        MigrationStep("2026_07_28_014", "quadras das competições", "banco:criar_tabela_competicao_quadras"),
        MigrationStep("2026_07_28_015", "agenda das competições", "banco:criar_tabela_competicao_agenda_config"),
        MigrationStep("2026_07_28_016", "rodadas das competições", "banco:criar_tabela_competicao_rodadas"),
        MigrationStep("2026_07_28_017", "eventos das partidas", "banco:criar_tabela_eventos"),
        MigrationStep("2026_07_28_018", "campos de sets das partidas", "banco:criar_campos_sets_partida"),
        MigrationStep("2026_07_28_019", "campos de jogo das partidas", "banco:criar_campos_jogo_partida"),
        MigrationStep("2026_07_28_020", "índices operacionais", "banco:criar_indices_desempenho"),
        MigrationStep("2026_07_29_021", "cadastro básico de partidas", "banco:criar_tabela_partidas"),
        MigrationStep("2026_07_29_022", "grupos e vínculos de equipes", "banco:criar_tabelas_grupos"),
        MigrationStep("2026_07_30_023", "fluxo de configuração inicial", "banco:garantir_schema_fluxo_configuracao_competicoes"),
        MigrationStep("2026_07_30_024", "premiação e destaques da competição", "banco:criar_tabelas_premiacao_destaques_competicao"),
    )


def _ensure_tracking_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vtp_schema_migrations (
                version TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                hostname TEXT NOT NULL DEFAULT '',
                process_id INTEGER
            )
            """
        )
    conn.commit()


def _applied_versions(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM vtp_schema_migrations")
        return {str(row["version"] if isinstance(row, dict) else row[0]) for row in cur.fetchall()}


def _mark_applied(conn, step: MigrationStep) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO vtp_schema_migrations
                (version, description, applied_at, hostname, process_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (version) DO NOTHING
            """,
            (
                step.version,
                step.description,
                datetime.now(timezone.utc),
                socket.gethostname()[:255],
                os.getpid(),
            ),
        )
    conn.commit()


def executar_migracoes(*, dry_run: bool = False, force: bool = False) -> list[dict[str, str]]:
    """Executa migrações pendentes sob um lock global do PostgreSQL."""
    resultado: list[dict[str, str]] = []
    steps = _steps()

    if dry_run:
        return [
            {"version": step.version, "description": step.description, "status": "pendente_desconhecido"}
            for step in steps
        ]

    from repositories.conexao import conectar

    with permitir_ddl_migracao(), conectar() as lock_conn:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (_LOCK_ID,))
        try:
            _ensure_tracking_table(lock_conn)
            applied = _applied_versions(lock_conn)

            for step in steps:
                if step.version in applied and not force:
                    resultado.append({"version": step.version, "description": step.description, "status": "ja_aplicada"})
                    continue

                step.resolve_runner()()
                _mark_applied(lock_conn, step)
                resultado.append({"version": step.version, "description": step.description, "status": "aplicada"})
        finally:
            try:
                with lock_conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_ID,))
                lock_conn.commit()
            except Exception:
                pass

    return resultado


def listar_migracoes() -> Iterable[MigrationStep]:
    return _steps()
