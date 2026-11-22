"""
Entrypoint FastAPI + Swagger/OpenAPI (/docs) para o pipeline Game & SaaS Telemetry ETL.
"""

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, status
from sqlalchemy.engine import Engine

from src.database import (
    create_db_engine,
    execute_analytical_queries,
    init_database,
    persist_etl_batch,
)
from src.etl_pipeline import transform_events_batch
from src.firebase_connector import FirestoreAckMode, FirestoreETLConnector
from src.schemas import (
    AnalyticsSummaryResponse,
    FirestoreSeedDocumentInput,
    FirestoreSyncRequest,
    FirestoreSyncResponse,
    IngestionResponse,
    TelemetryBatchInput,
    TelemetryEventInput,
)


def get_engine() -> Engine:
    """Dependency provider do SQLAlchemy Engine para facilitar override em testes."""
    return getattr(app.state, "db_engine", None) or create_db_engine()


def get_firestore_connector() -> FirestoreETLConnector:
    """Dependency provider do conector Dual-Mode do Firebase Cloud Firestore."""
    connector = getattr(app.state, "firestore_connector", None)
    if connector is None:
        connector = FirestoreETLConnector()
        app.state.firestore_connector = connector
    return connector


@asynccontextmanager
async def lifespan(application: FastAPI):
    engine = create_db_engine()
    init_database(engine)
    application.state.db_engine = engine
    application.state.firestore_connector = FirestoreETLConnector()
    yield
    engine.dispose()


app = FastAPI(
    title="Game & SaaS Telemetry ETL API",
    description=(
        "Pipeline de Engenharia de Dados RESTful para ingestão de telemetria de Jogos "
        "Eletrônicos e Plataformas SaaS Enterprise, com extração NoSQL (**Firebase Cloud Firestore**), "
        "sanitização determinística **Zero-Exposure (LGPD)**, deduplicação idempotente e "
        "Data Warehouse **SQL Star Schema**."
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["Healthcheck"])
def healthcheck(
    engine: Engine = Depends(get_engine),
    firestore_connector: FirestoreETLConnector = Depends(get_firestore_connector),
):
    """Verifica disponibilidade da API, banco SQL e modo do conector Firebase Firestore."""
    init_database(engine)
    return {
        "status": "healthy",
        "service": "game-saas-telemetry-etl-api",
        "zero_exposure_lgpd": "enabled",
        "schema": "star_schema_olap",
        "firebase_firestore_mode": firestore_connector.backend_mode,
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


@app.post(
    "/api/v1/telemetry/firebase/seed",
    status_code=status.HTTP_201_CREATED,
    tags=["Firebase Firestore ETL"],
    summary="Popula a coleção do Firestore Simulator com documentos NoSQL aninhados (Games & SaaS)",
)
def seed_firestore_collection(
    collection_name: str = "telemetry_raw_events",
    custom_doc: FirestoreSeedDocumentInput | None = None,
    firestore_connector: FirestoreETLConnector = Depends(get_firestore_connector),
):
    """
    Insere documentos de demonstração (ou um documento customizado aninhado estilo Firebase/GA4)
    no Cloud Firestore Simulator para testar a extração NoSQL -> Zero-Exposure -> SQL.
    """
    if custom_doc is not None:
        firestore_connector.memory_store.upsert_document(
            collection_name=collection_name,
            doc_id=custom_doc.doc_id,
            data=custom_doc.data,
        )
        seeded = 1
    else:
        seeded = firestore_connector.memory_store.seed_default_collection(collection_name)

    return {
        "status": "seeded",
        "collection_name": collection_name,
        "seeded_documents": seeded,
        "total_documents_in_collection": firestore_connector.memory_store.count_documents(
            collection_name
        ),
    }


@app.post(
    "/api/v1/telemetry/firebase/sync",
    response_model=FirestoreSyncResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Firebase Firestore ETL"],
    summary="Extrai documentos NoSQL do Cloud Firestore, aplica Zero-Exposure e carrega no Star Schema SQL",
)
def sync_from_firebase_firestore(
    sync_req: FirestoreSyncRequest,
    engine: Engine = Depends(get_engine),
    firestore_connector: FirestoreETLConnector = Depends(get_firestore_connector),
) -> FirestoreSyncResponse:
    """
    Executa o fluxo completo de Engenharia de Dados com Firebase:
    1. **Extract (Pull)**: Lê documentos da coleção do Cloud Firestore (`doc.id` + mapas aninhados).
    2. **Normalize (Flatten)**: Converte sub-mapas (`user`, `product`, `tenant`, `metrics`, `context`)
       para o contrato tabular.
    3. **Transform (Zero-Exposure + Pandas)**: Mascaramento irreversível SHA-256 de PII e deduplicação.
    4. **Load (Star Schema SQL)**: Persistência idempotente nas tabelas dimensionais e fato.
    5. **Acknowledge / Purge PII**: Marca como sincronizado e expurga PII na origem NoSQL (`ack_mode`).
    """
    init_database(engine)
    ack_enum = FirestoreAckMode(sync_req.ack_mode)
    normalized_events, ack_count = firestore_connector.extract_and_acknowledge(
        collection_name=sync_req.collection_name,
        limit=sync_req.limit,
        only_unsynced=sync_req.only_unsynced,
        ack_mode=ack_enum,
    )

    etl_result = transform_events_batch(normalized_events)
    inserted, ignored = persist_etl_batch(etl_result, engine=engine)

    return FirestoreSyncResponse(
        status="success",
        firestore_backend_mode=firestore_connector.backend_mode,
        collection_name=sync_req.collection_name,
        ack_mode=sync_req.ack_mode,
        extracted_from_firestore=len(normalized_events),
        acknowledged_in_firestore=ack_count,
        deduplicated_in_batch=etl_result.deduplicated_in_batch,
        inserted_in_warehouse=inserted,
        ignored_duplicates_in_warehouse=ignored,
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
