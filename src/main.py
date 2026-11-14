"""
Entrypoint FastAPI + Swagger/OpenAPI (/docs) para o pipeline Game & SaaS Telemetry ETL.
"""

from contextlib import asynccontextmanager
from typing import Optional
from fastapi import Depends, FastAPI, status
from sqlalchemy.engine import Engine

from src.database import (
    create_db_engine,
    execute_analytical_queries,
    init_database,
    persist_etl_batch,
)
from src.etl_pipeline import transform_events_batch
from src.schemas import (
    AnalyticsSummaryResponse,
    IngestionResponse,
    TelemetryBatchInput,
    TelemetryEventInput,
)


def get_engine() -> Engine:
    """Dependency provider do SQLAlchemy Engine para facilitar override em testes."""
    return getattr(app.state, "db_engine", None) or create_db_engine()


@asynccontextmanager
async def lifespan(application: FastAPI):
    engine = create_db_engine()
    init_database(engine)
    application.state.db_engine = engine
    yield
    engine.dispose()


app = FastAPI(
    title="Game & SaaS Telemetry ETL API",
    description=(
        "Pipeline de Engenharia de Dados RESTful para ingestão de telemetria de Jogos "
        "Eletrônicos e Plataformas SaaS Enterprise, com sanitização determinística "
        "**Zero-Exposure (LGPD)**, deduplicação idempotente e Data Warehouse **SQL Star Schema**."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["Healthcheck"])
def healthcheck(engine: Engine = Depends(get_engine)):
    """Verifica disponibilidade da API e conectividade com o banco SQL."""
    init_database(engine)
    return {
        "status": "healthy",
        "service": "game-saas-telemetry-etl-api",
        "zero_exposure_lgpd": "enabled",
        "schema": "star_schema_olap",
    }


@app.post(
    "/api/v1/telemetry/events",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Telemetry Ingestion"],
    summary="Ingere um único evento de telemetria com Zero-Exposure e Idempotência",
)
def ingest_single_event(
    event: TelemetryEventInput,
    engine: Engine = Depends(get_engine),
) -> IngestionResponse:
    """
    Valida o contrato via Pydantic v2 (rejeita malformações com HTTP 422),
    anonimiza dados sensíveis (e-mail, CPF, IP e nome) via SHA-256 + Regex
    e persiste no Star Schema de forma idempotente (`event_id`).
    """
    init_database(engine)
    etl_result = transform_events_batch([event.model_dump()])
    inserted, ignored = persist_etl_batch(etl_result, engine=engine)

    return IngestionResponse(
        status="success",
        received_count=1,
        deduplicated_in_batch=etl_result.deduplicated_in_batch,
        inserted_count=inserted,
        ignored_duplicates_in_db=ignored,
        zero_exposure_applied=True,
        sample_user_hashes=etl_result.sample_user_hashes,
    )


@app.post(
    "/api/v1/telemetry/batch",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Telemetry Ingestion"],
    summary="Ingere um lote (Batch ETL) de eventos com higienização Pandas e deduplicação",
)
def ingest_batch_events(
    batch: TelemetryBatchInput,
    engine: Engine = Depends(get_engine),
) -> IngestionResponse:
    """
    Processa lotes de até 5.000 eventos simultâneos aplicando:
    1. Sanitização Zero-Exposure (LGPD)
    2. Deduplicação em memória via Pandas (`event_id`)
    3. Persistência transacional idempotente no modelo relacional Star Schema
    """
    init_database(engine)
    raw_list = [evt.model_dump() for evt in batch.events]
    etl_result = transform_events_batch(raw_list)
    inserted, ignored = persist_etl_batch(etl_result, engine=engine)

    return IngestionResponse(
        status="success",
        received_count=etl_result.received_count,
        deduplicated_in_batch=etl_result.deduplicated_in_batch,
        inserted_count=inserted,
        ignored_duplicates_in_db=ignored,
        zero_exposure_applied=True,
        sample_user_hashes=etl_result.sample_user_hashes,
    )


@app.get(
    "/api/v1/analytics/summary",
    response_model=AnalyticsSummaryResponse,
    tags=["OLAP Analytics"],
    summary="Consolida métricas estratégicas SQL (DAU/MAU, Latência P95, Receita e Retenção)",
)
def get_analytics_summary(
    engine: Engine = Depends(get_engine),
) -> AnalyticsSummaryResponse:
    """
    Executa as queries analíticas puras de `sql/02_analytical_queries.sql` sobre o
    Star Schema para alimentar dashboards estratégicos de produto e engenharia.
    """
    init_database(engine)
    metrics = execute_analytical_queries(engine=engine)
    return AnalyticsSummaryResponse(**metrics)
