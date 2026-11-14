"""
Pipeline ETL em lote com Pandas:
- Aplica camada Zero-Exposure (sanitizer.py)
- Trata valores nulos e anomalias numéricas
- Deduplica eventos pelo event_id (idempotência em memória)
- Separa DataFrames para o Star Schema (dim_tenants, dim_products, dim_users, fact_telemetry_events)
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd

from src.sanitizer import sanitize_event_payload

VALID_CATEGORIES = {"GAME", "SAAS_ENTERPRISE"}
VALID_TIERS = {"FREE", "PRO", "ENTERPRISE"}
VALID_EVENT_TYPES = {"MATCH_END", "API_CALL", "PURCHASE", "ERROR_TIMEOUT"}


@dataclass
class ETLBatchResult:
    """Resultado estruturado da transformação em lote Pandas."""

    received_count: int
    deduplicated_in_batch: int
    dim_tenants_df: pd.DataFrame
    dim_products_df: pd.DataFrame
    dim_users_df: pd.DataFrame
    fact_events_df: pd.DataFrame
    sample_user_hashes: List[str]


def transform_events_batch(
    raw_events: List[Dict[str, Any]],
    salt: Optional[str] = None,
) -> ETLBatchResult:
    """
    Executa o pipeline completo de higienização, tipagem e deduplicação em lote via Pandas.
    """
    received_count = len(raw_events)
    if received_count == 0:
        empty_df = pd.DataFrame()
        return ETLBatchResult(
            received_count=0,
            deduplicated_in_batch=0,
            dim_tenants_df=empty_df,
            dim_products_df=empty_df,
            dim_users_df=empty_df,
            fact_events_df=empty_df,
            sample_user_hashes=[],
        )

    # 1. Camada Zero-Exposure em cada registro de entrada
    sanitized_rows: List[Dict[str, Any]] = []
    for item in raw_events:
        if hasattr(item, "model_dump"):
            payload = item.model_dump()
        else:
            payload = dict(item)
        sanitized_rows.append(sanitize_event_payload(payload, salt=salt))

    df = pd.DataFrame(sanitized_rows)

    # 2. Deduplicação determinística por event_id dentro do lote
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["event_id"], keep="first").copy()
    deduplicated_in_batch = before_dedup - len(df)

    # 3. Higienização, tratamento de nulos e coerção de tipos numéricos
    df["session_duration_sec"] = (
        pd.to_numeric(df["session_duration_sec"], errors="coerce")
        .fillna(0)
        .clip(lower=0, upper=86400)
        .astype(int)
    )
    df["latency_ms"] = (
        pd.to_numeric(df["latency_ms"], errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0, upper=300000.0)
        .round(2)
        .astype(float)
    )
    df["revenue_brl"] = (
        pd.to_numeric(df["revenue_brl"], errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0, upper=1000000.0)
        .round(2)
        .astype(float)
    )

    # Regra de consistência: apenas eventos PURCHASE geram receita positiva contabilizável
    df.loc[df["event_type"] != "PURCHASE", "revenue_brl"] = 0.0

    # Normalização de categorias e enums caso venham de ingestão bruta
    df["category"] = df["category"].where(df["category"].isin(VALID_CATEGORIES), "GAME")
    df["tier"] = df["tier"].where(df["tier"].isin(VALID_TIERS), "FREE")
    df["event_type"] = df["event_type"].where(df["event_type"].isin(VALID_EVENT_TYPES), "API_CALL")

    # 4. Padronização de timestamps em formato ISO SQL (YYYY-MM-DD HH:MM:SS)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    parsed_dates = pd.to_datetime(df["occurred_at"], errors="coerce", utc=True)
    df["occurred_at"] = parsed_dates.dt.strftime("%Y-%m-%d %H:%M:%S").fillna(now_iso)

    # 5. Construção dos DataFrames Dimensionais e Fato (Star Schema)
    dim_tenants_df = (
        df[["tenant_id", "tenant_name", "category"]]
        .rename(columns={"category": "segment"})
        .drop_duplicates(subset=["tenant_id"], keep="first")
        .copy()
    )
    dim_tenants_df["created_at"] = now_iso

    dim_products_df = (
        df[["product_id", "product_name", "category"]]
        .drop_duplicates(subset=["product_id"], keep="first")
        .copy()
    )
    dim_products_df["created_at"] = now_iso

    # Para dim_users, capturamos o menor timestamp observado como first_seen_at
    dim_users_df = (
        df.sort_values("occurred_at", ascending=True)
        .groupby("user_hash", as_index=False)
        .first()[["user_hash", "tenant_id", "tier", "occurred_at"]]
        .rename(columns={"occurred_at": "first_seen_at"})
    )

    fact_events_df = df[
        [
            "event_id",
            "product_id",
            "user_hash",
            "event_type",
            "session_duration_sec",
            "latency_ms",
            "revenue_brl",
            "sanitized_metadata",
            "occurred_at",
        ]
    ].copy()

    sample_hashes = fact_events_df["user_hash"].head(5).tolist()

    return ETLBatchResult(
        received_count=received_count,
        deduplicated_in_batch=deduplicated_in_batch,
        dim_tenants_df=dim_tenants_df,
        dim_products_df=dim_products_df,
        dim_users_df=dim_users_df,
        fact_events_df=fact_events_df,
        sample_user_hashes=sample_hashes,
    )
