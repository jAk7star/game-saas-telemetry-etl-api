"""
Contratos Pydantic v2 para validação rigorosa de entrada (HTTP 422) e respostas da API.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProductCategory(str, Enum):
    GAME = "GAME"
    SAAS_ENTERPRISE = "SAAS_ENTERPRISE"


class UserTier(str, Enum):
    FREE = "FREE"
    PRO = "PRO"
    ENTERPRISE = "ENTERPRISE"


class EventType(str, Enum):
    MATCH_END = "MATCH_END"
    API_CALL = "API_CALL"
    PURCHASE = "PURCHASE"
    ERROR_TIMEOUT = "ERROR_TIMEOUT"


class TelemetryEventInput(BaseModel):
    """
    Contrato de entrada de evento de telemetria (Game ou SaaS).
    Campos identificáveis (PII) são aceitos apenas no payload de entrada e
    imediatamente convertidos/mascarados pela camada Zero-Exposure (sanitizer.py)
    antes de qualquer persistência.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_id: str = Field(
        ...,
        min_length=4,
        max_length=64,
        description="Identificador único determinístico do evento para garantir idempotência.",
        examples=["evt_9f8a7b6c5d4e3f2a1b0c"],
    )
    tenant_id: str = Field(
        ...,
        min_length=2,
        max_length=64,
        description="Identificador do tenant (organização ou estúdio).",
        examples=["tenant_valkyrie_studios"],
    )
    tenant_name: str | None = Field(
        default="Default Tenant",
        max_length=120,
        description="Nome comercial do tenant.",
    )
    product_id: str = Field(
        ...,
        min_length=2,
        max_length=64,
        description="Identificador único do produto (Game ou SaaS).",
        examples=["prod_cyber_arena_br"],
    )
    product_name: str = Field(
        default="Unknown Product",
        min_length=2,
        max_length=120,
        description="Nome do produto monitorado.",
    )
    category: ProductCategory = Field(
        ...,
        description="Categoria do ecossistema ('GAME' ou 'SAAS_ENTERPRISE').",
    )
    user_identifier: str = Field(
        ...,
        min_length=2,
        max_length=255,
        description="Identificador bruto do usuário (e-mail, CPF ou ID). Será anonimizado via SHA-256.",
        examples=["player.one@example.com"],
    )
    user_name: str | None = Field(
        default=None,
        max_length=120,
        description="Nome opcional do usuário (descartado/mascarado na camada Zero-Exposure).",
    )
    user_cpf: str | None = Field(
        default=None,
        max_length=20,
        description="CPF opcional do usuário (descartado/mascarado na camada Zero-Exposure).",
    )
    ip_address: str | None = Field(
        default=None,
        max_length=64,
        description="Endereço IP de origem (mascarado na camada Zero-Exposure).",
    )
    tier: UserTier = Field(
        default=UserTier.FREE,
        description="Plano de assinatura ou nível do usuário ('FREE', 'PRO', 'ENTERPRISE').",
    )
    event_type: EventType = Field(
        ...,
        description="Tipo do evento ('MATCH_END', 'API_CALL', 'PURCHASE', 'ERROR_TIMEOUT').",
    )
    session_duration_sec: int | None = Field(
        default=0,
        ge=0,
        le=86400,
        description="Duração da sessão em segundos (>= 0).",
    )
    latency_ms: float | None = Field(
        default=0.0,
        ge=0.0,
        le=300000.0,
        description="Latência de rede/API medida em milissegundos (>= 0).",
    )
    revenue_brl: float | None = Field(
        default=0.0,
        ge=0.0,
        le=1000000.0,
        description="Receita associada ao evento em Reais (BRL >= 0).",
    )
    raw_metadata: str | None = Field(
        default=None,
        max_length=1000,
        description="Texto livre de log/diagnóstico (passará por sanitização Regex contra PII).",
    )
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp UTC de ocorrência do evento.",
    )

    @field_validator("event_id", "tenant_id", "product_id")
    @classmethod
    def validate_no_spaces_in_ids(cls, value: str) -> str:
        if " " in value:
            raise ValueError("Identificadores técnicos não podem conter espaços em branco.")
        return value


class TelemetryBatchInput(BaseModel):
    """Contrato para ingestão em lote (Batch ETL) de múltiplos eventos."""

    model_config = ConfigDict(extra="forbid")

    events: list[TelemetryEventInput] = Field(
        ...,
        min_length=1,
        max_length=5000,
        description="Lista de eventos de telemetria para processamento em lote.",
    )


class IngestionResponse(BaseModel):
    """Resposta padronizada de ingestão e processamento ETL."""

    status: str = Field(default="success")
    received_count: int = Field(..., ge=0)
    deduplicated_in_batch: int = Field(..., ge=0)
    inserted_count: int = Field(..., ge=0)
    ignored_duplicates_in_db: int = Field(..., ge=0)
    zero_exposure_applied: bool = Field(default=True)
    sample_user_hashes: list[str] = Field(default_factory=list)


class AnalyticsSummaryResponse(BaseModel):
    """Resposta consolidada das consultas analíticas OLAP."""

    total_events: int
    total_unique_users: int
    dau_mau_metrics: list[dict[str, Any]]
    latency_p95_metrics: list[dict[str, Any]]
    revenue_metrics: list[dict[str, Any]]
    retention_metrics: list[dict[str, Any]]


class FirestoreSyncRequest(BaseModel):
    """Contrato para extração ativa (Pull ETL) de uma coleção do Cloud Firestore."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    collection_name: str = Field(
        default="telemetry_raw_events",
        min_length=2,
        max_length=120,
        description="Nome da coleção de staging no Cloud Firestore.",
    )
    limit: int = Field(
        default=500,
        ge=1,
        le=5000,
        description="Quantidade máxima de documentos a extrair por lote.",
    )
    only_unsynced: bool = Field(
        default=True,
        description="Se True, ignora documentos já marcados com `etl_synced=True` no Firestore.",
    )
    ack_mode: str = Field(
        default="mark_synced_and_strip_pii",
        pattern="^(read_only|mark_synced_and_strip_pii|delete_after_ingest)$",
        description=(
            "Política pós-extração no Firestore: 'read_only', "
            "'mark_synced_and_strip_pii' (expurga PII na origem NoSQL) ou 'delete_after_ingest'."
        ),
    )


class FirestoreSeedDocumentInput(BaseModel):
    """Documento NoSQL (aninhado estilo Firebase/GA4 ou plano) para popular o Firestore Simulator."""

    doc_id: str = Field(
        ...,
        min_length=4,
        max_length=64,
        description="ID determinístico do documento no Cloud Firestore (`doc.id`).",
    )
    data: dict[str, Any] = Field(
        ...,
        description="Mapa de dados do documento Firestore (suporta sub-mapas `user`, `product`, `tenant`, `metrics`, `context`).",
    )


class FirestoreSyncResponse(BaseModel):
    """Resposta detalhada da sincronização Cloud Firestore -> Zero-Exposure -> Star Schema SQL."""

    status: str = Field(default="success")
    firestore_backend_mode: str = Field(
        ...,
        description="Modo utilizado ('firebase_admin_sdk' ou 'in_memory_simulator').",
    )
    collection_name: str
    ack_mode: str
    extracted_from_firestore: int = Field(..., ge=0)
    acknowledged_in_firestore: int = Field(..., ge=0)
    deduplicated_in_batch: int = Field(..., ge=0)
    inserted_in_warehouse: int = Field(..., ge=0)
    ignored_duplicates_in_warehouse: int = Field(..., ge=0)
    zero_exposure_applied: bool = Field(default=True)
    sample_user_hashes: list[str] = Field(default_factory=list)
