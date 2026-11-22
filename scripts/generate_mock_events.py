#!/usr/bin/env python3
"""
Gerador Sintético de 2.000 Eventos Mistos (Games & SaaS Enterprise) com Sujeira Intencional:
- E-mails reais simulados, CPFs, nomes completos e endereços IPv4 (testando Zero-Exposure LGPD)
- Duplicatas intencionais de `event_id` (testando idempotência de rede e lote)
- Anomalias numéricas e valores nulos em duração/latência (testando higienização Pandas)
- Distribuição temporal em múltiplos dias para cálculo realista de DAU/MAU, Retenção e P95
"""

import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Garante importação do pacote `src` ao rodar diretamente via CLI
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.database import (
    create_db_engine,
    execute_analytical_queries,
    init_database,
    persist_etl_batch,
)
from src.etl_pipeline import transform_events_batch

PRODUCTS_CATALOG = [
    {
        "tenant_id": "tenant_valkyrie_games",
        "tenant_name": "Valkyrie Interactive Studios",
        "product_id": "prod_cyber_arena_br",
        "product_name": "Cyber Arena Battle Royale",
        "category": "GAME",
    },
    {
        "tenant_id": "tenant_valkyrie_games",
        "tenant_name": "Valkyrie Interactive Studios",
        "product_id": "prod_starlight_odyssey",
        "product_name": "Starlight Odyssey RPG",
        "category": "GAME",
    },
    {
        "tenant_id": "tenant_nexus_cloud",
        "tenant_name": "Nexus Enterprise Cloud SaaS",
        "product_id": "prod_dataops_hub",
        "product_name": "Nexus DataOps Analytics Hub",
        "category": "SAAS_ENTERPRISE",
    },
    {
        "tenant_id": "tenant_nexus_cloud",
        "tenant_name": "Nexus Enterprise Cloud SaaS",
        "product_id": "prod_sec_gateway_api",
        "product_name": "Nexus ZeroTrust API Gateway",
        "category": "SAAS_ENTERPRISE",
    },
]

SAMPLE_FIRST_NAMES = [
    "Lucas",
    "Mariana",
    "Gabriel",
    "Beatriz",
    "Rafael",
    "Fernanda",
    "Thiago",
    "Camila",
    "Bruno",
    "Juliana",
]
SAMPLE_LAST_NAMES = [
    "Silva",
    "Oliveira",
    "Santos",
    "Souza",
    "Pereira",
    "Costa",
    "Rodrigues",
    "Almeida",
    "Nascimento",
    "Lima",
]
TIERS = ["FREE", "PRO", "ENTERPRISE"]


def build_synthetic_users(num_users: int = 180, seed: int = 42) -> list[dict[str, str]]:
    rng = random.Random(seed)
    users = []
    for idx in range(1, num_users + 1):
        first = rng.choice(SAMPLE_FIRST_NAMES)
        last = rng.choice(SAMPLE_LAST_NAMES)
        full_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{idx}@cliente-corp.com.br"
        cpf = (
            f"{rng.randint(100, 999)}.{rng.randint(100, 999)}."
            f"{rng.randint(100, 999)}-{rng.randint(10, 99)}"
        )
        ip = f"189.{rng.randint(10, 250)}.{rng.randint(1, 254)}.{rng.randint(1, 254)}"
        tier = rng.choices(TIERS, weights=[0.55, 0.30, 0.15], k=1)[0]
        users.append(
            {
                "user_identifier": email,
                "user_name": full_name,
                "user_cpf": cpf,
                "ip_address": ip,
                "tier": tier,
            }
        )
    return users


def generate_mock_events(total_events: int = 2000, seed: int = 42) -> list[dict[str, Any]]:
    """
    Gera `total_events` eventos mistos de Games e SaaS com anomalias controladas:
    - ~5% de eventos duplicados (mesmo `event_id` simulando retry de rede)
    - ~8% de campos numéricos nulos ou sujos (tratados pelo Pandas ETL)
    - Vazamento intencional de PII (e-mail, CPF, IP e nome) em `raw_metadata`
    """
    rng = random.Random(seed)
    users = build_synthetic_users(num_users=180, seed=seed)
    base_time = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)

    events: list[dict[str, Any]] = []
    unique_target = int(total_events * 0.95)

    for i in range(1, unique_target + 1):
        product = rng.choice(PRODUCTS_CATALOG)
        user = rng.choice(users)

        if product["category"] == "GAME":
            event_type = rng.choices(
                ["MATCH_END", "PURCHASE", "ERROR_TIMEOUT", "API_CALL"],
                weights=[0.60, 0.15, 0.08, 0.17],
                k=1,
            )[0]
            session_sec = (
                rng.randint(180, 3600) if event_type != "API_CALL" else rng.randint(5, 120)
            )
            base_latency = rng.uniform(18.0, 95.0)
        else:
            event_type = rng.choices(
                ["API_CALL", "PURCHASE", "ERROR_TIMEOUT", "MATCH_END"],
                weights=[0.70, 0.12, 0.10, 0.08],
                k=1,
            )[0]
            session_sec = rng.randint(30, 1800)
            base_latency = rng.uniform(12.0, 75.0)

        # Simula picos de latência (tail latency P95/P99) em ERROR_TIMEOUT
        if event_type == "ERROR_TIMEOUT":
            latency_ms = round(rng.uniform(450.0, 2400.0), 2)
        else:
            latency_ms = round(base_latency, 2)

        # Receita apenas para PURCHASE ( mais algumas sujeiras em outros eventos para o ETL zerar)
        if event_type == "PURCHASE":
            revenue_brl = round(rng.choice([19.90, 49.90, 99.90, 249.00, 890.00]), 2)
        elif i % 25 == 0:
            # Sujeira intencional: receita atribuída a evento não-PURCHASE
            revenue_brl = 15.00
        else:
            revenue_brl = 0.0

        # Sujeira intencional: valores nulos em ~8% dos registros
        if i % 13 == 0:
            session_sec = None
        if i % 17 == 0:
            latency_ms = None

        day_offset = rng.randint(0, 13)
        minute_offset = rng.randint(0, 1439)
        occurred_at = (base_time + timedelta(days=day_offset, minutes=minute_offset)).isoformat()

        # Sujeira intencional de PII em texto livre
        dirty_metadata = (
            f"Auth user_name={user['user_name']} email={user['user_identifier']} "
            f"cpf={user['user_cpf']} src_ip={user['ip_address']} event={event_type}"
        )

        # Varia formato de user_identifier (às vezes caixa alta/espaços para testar normalização)
        raw_identifier = (
            f"  {user['user_identifier'].upper()}  " if i % 7 == 0 else user["user_identifier"]
        )

        events.append(
            {
                "event_id": f"evt_{i:06d}",
                "tenant_id": product["tenant_id"],
                "tenant_name": product["tenant_name"],
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "category": product["category"],
                "user_identifier": raw_identifier,
                "user_name": user["user_name"],
                "user_cpf": user["user_cpf"],
                "ip_address": user["ip_address"],
                "tier": user["tier"],
                "event_type": event_type,
                "session_duration_sec": session_sec,
                "latency_ms": latency_ms,
                "revenue_brl": revenue_brl,
                "raw_metadata": dirty_metadata,
                "occurred_at": occurred_at,
            }
        )

    # Preenche os 5% restantes com duplicatas exatas de event_id (simulando retentativas de rede)
    duplicates_needed = total_events - len(events)
    for _ in range(duplicates_needed):
        dup_event = dict(rng.choice(events))
        events.append(dup_event)

    rng.shuffle(events)
    return events


def main() -> None:
    print("=" * 72)
    print("🚀 GAME & SAAS TELEMETRY ETL — GERADOR DE 2.000 EVENTOS SINTÉTICOS")
    print("=" * 72)

    raw_events = generate_mock_events(total_events=2000, seed=42)
    print(f"[1/4] Eventos brutos gerados (com PII e duplicatas de rede): {len(raw_events)}")

    engine = create_db_engine()
    init_database(engine)
    print(
        "[2/4] Star Schema inicializado (dim_tenants, dim_products, dim_users, fact_telemetry_events)"
    )

    etl_result = transform_events_batch(raw_events)
    print(
        f"[3/4] Pipeline Pandas + Zero-Exposure concluído:\n"
        f"      • Recebidos no lote: {etl_result.received_count}\n"
        f"      • Deduplicados em memória (Pandas): {etl_result.deduplicated_in_batch}\n"
        f"      • Prontos para persistência única: {len(etl_result.fact_events_df)}"
    )

    inserted, ignored_db = persist_etl_batch(etl_result, engine=engine)
    print(
        f"[4/4] Persistência Relacional SQL concluída:\n"
        f"      • Novos registros inseridos: {inserted}\n"
        f"      • Duplicatas ignoradas pelo banco (PK event_id): {ignored_db}"
    )

    summary = execute_analytical_queries(engine=engine)
    print("\n📊 RESUMO ANALÍTICO OLAP (SQL Star Schema):")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
