"""
Camada de Persistência Relacional SQL (SQLAlchemy 2.0):
- Inicializa o Star Schema a partir de `sql/01_schema_ddl.sql`
- Garante ingestão idempotente via chave primária (`event_id`) com ON CONFLICT DO NOTHING
- Executa as consultas analíticas OLAP de `sql/02_analytical_queries.sql`
"""

import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.etl_pipeline import ETLBatchResult

BASE_DIR = Path(__file__).resolve().parent.parent
DDL_FILE_PATH = BASE_DIR / "sql" / "01_schema_ddl.sql"
ANALYTICAL_SQL_PATH = BASE_DIR / "sql" / "02_analytical_queries.sql"


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'telemetry_warehouse.db'}")


def create_db_engine(database_url: str | None = None) -> Engine:
    url = database_url or get_database_url()
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, connect_args=connect_args, future=True)


def _split_sql_statements(sql_script: str) -> list[str]:
    """Remove comentários de linha (`--`) e divide o script SQL em instruções executáveis."""
    lines = [line for line in sql_script.splitlines() if not line.strip().startswith("--")]
    cleaned_script = "\n".join(lines)
    return [stmt.strip() for stmt in cleaned_script.split(";") if stmt.strip()]


def init_database(engine: Engine | None = None) -> Engine:
    """Executa o script DDL (`sql/01_schema_ddl.sql`) para criar tabelas e índices."""
    db_engine = engine or create_db_engine()
    ddl_script = DDL_FILE_PATH.read_text(encoding="utf-8")

    with db_engine.begin() as conn:
        if db_engine.dialect.name == "sqlite":
            conn.exec_driver_sql("PRAGMA foreign_keys = ON;")
        for statement in _split_sql_statements(ddl_script):
            conn.exec_driver_sql(statement)

    return db_engine


def persist_etl_batch(
    batch_result: ETLBatchResult,
    engine: Engine | None = None,
) -> tuple[int, int]:
    """
    Persiste dimensões e fatos no Star Schema de maneira transacional e idempotente.
    Retorna `(inserted_count, ignored_duplicates_in_db)`.
    """
    if batch_result.fact_events_df.empty:
        return 0, 0

    db_engine = engine or create_db_engine()

    tenants_records = batch_result.dim_tenants_df.to_dict(orient="records")
    products_records = batch_result.dim_products_df.to_dict(orient="records")
    users_records = batch_result.dim_users_df.to_dict(orient="records")
    facts_records = batch_result.fact_events_df.to_dict(orient="records")

    upsert_tenants_sql = text(
        """
        INSERT INTO dim_tenants (tenant_id, tenant_name, segment, created_at)
        VALUES (:tenant_id, :tenant_name, :segment, :created_at)
        ON CONFLICT (tenant_id) DO NOTHING
        """
    )

    upsert_products_sql = text(
        """
        INSERT INTO dim_products (product_id, product_name, category, created_at)
        VALUES (:product_id, :product_name, :category, :created_at)
        ON CONFLICT (product_id) DO NOTHING
        """
    )

    upsert_users_sql = text(
        """
        INSERT INTO dim_users (user_hash, tenant_id, tier, first_seen_at)
        VALUES (:user_hash, :tenant_id, :tier, :first_seen_at)
        ON CONFLICT (user_hash) DO UPDATE SET
            tier = excluded.tier
        """
    )

    insert_fact_sql = text(
        """
        INSERT INTO fact_telemetry_events (
            event_id,
            product_id,
            user_hash,
            event_type,
            session_duration_sec,
            latency_ms,
            revenue_brl,
            sanitized_metadata,
            occurred_at
        ) VALUES (
            :event_id,
            :product_id,
            :user_hash,
            :event_type,
            :session_duration_sec,
            :latency_ms,
            :revenue_brl,
            :sanitized_metadata,
            :occurred_at
        )
        ON CONFLICT (event_id) DO NOTHING
        """
    )

    inserted_count = 0
    with db_engine.begin() as conn:
        if tenants_records:
            conn.execute(upsert_tenants_sql, tenants_records)
        if products_records:
            conn.execute(upsert_products_sql, products_records)
        if users_records:
            conn.execute(upsert_users_sql, users_records)

        # Contabiliza exatamente quantos eventos novos foram inseridos (idempotência)
        before_count = conn.execute(text("SELECT COUNT(*) FROM fact_telemetry_events")).scalar_one()

        conn.execute(insert_fact_sql, facts_records)

        after_count = conn.execute(text("SELECT COUNT(*) FROM fact_telemetry_events")).scalar_one()

        inserted_count = int(after_count - before_count)

    ignored_duplicates = len(facts_records) - inserted_count
    return inserted_count, ignored_duplicates


def execute_analytical_queries(engine: Engine | None = None) -> dict[str, Any]:
    """
    Executa as 4 consultas analíticas SQL de `sql/02_analytical_queries.sql`
    e retorna os indicadores consolidados para consumo via API/BI.
    """
    db_engine = engine or create_db_engine()
    raw_sql = ANALYTICAL_SQL_PATH.read_text(encoding="utf-8")
    statements = _split_sql_statements(raw_sql)

    with db_engine.connect() as conn:
        total_events = conn.execute(text("SELECT COUNT(*) FROM fact_telemetry_events")).scalar_one()
        total_users = conn.execute(text("SELECT COUNT(*) FROM dim_users")).scalar_one()

        results: list[list[dict[str, Any]]] = []
        for stmt in statements:
            rows = conn.execute(text(stmt)).mappings().all()
            results.append([dict(r) for r in rows])

    return {
        "total_events": int(total_events),
        "total_unique_users": int(total_users),
        "dau_mau_metrics": results[0] if len(results) > 0 else [],
        "latency_p95_metrics": results[1] if len(results) > 1 else [],
        "revenue_metrics": results[2] if len(results) > 2 else [],
        "retention_metrics": results[3] if len(results) > 3 else [],
    }
