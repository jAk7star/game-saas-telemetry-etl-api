"""
Testes de Integração da API FastAPI, Pipeline ETL Pandas e Persistência Idempotente SQL.
"""

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import text

from scripts.generate_mock_events import generate_mock_events
from src.database import create_db_engine, init_database, persist_etl_batch
from src.etl_pipeline import transform_events_batch
from src.main import app, get_engine


@pytest.fixture()
def test_client(tmp_path: Path):
    """Cria um banco SQLite isolado por teste e injeta o engine na aplicação FastAPI."""
    db_file = tmp_path / "test_telemetry.db"
    engine = create_db_engine(f"sqlite:///{db_file}")
    init_database(engine)

    app.dependency_overrides[get_engine] = lambda: engine
    with TestClient(app) as client:
        yield client, engine
    app.dependency_overrides.clear()
    engine.dispose()


def test_healthcheck_endpoint(test_client):
    client, _ = test_client
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["zero_exposure_lgpd"] == "enabled"


def test_single_event_ingestion_idempotency_and_zero_exposure(test_client):
    """
    Reenviar o mesmo `event_id` pela rede não pode duplicar registros no banco SQL
    e nenhum PII pode ser persistido.
    """
    client, engine = test_client
    payload = {
        "event_id": "evt_idemp_0001",
        "tenant_id": "tenant_valkyrie",
        "tenant_name": "Valkyrie Studios",
        "product_id": "prod_cyber_arena",
        "product_name": "Cyber Arena",
        "category": "GAME",
        "user_identifier": "gabriel.santos@gamer.com",
        "user_name": "Gabriel Santos",
        "user_cpf": "111.222.333-44",
        "ip_address": "192.168.15.20",
        "tier": "PRO",
        "event_type": "PURCHASE",
        "session_duration_sec": 1200,
        "latency_ms": 35.4,
        "revenue_brl": 49.90,
        "raw_metadata": "Checkout Gabriel Santos cpf=111.222.333-44 email=gabriel.santos@gamer.com",
        "occurred_at": "2026-11-12T18:00:00Z",
    }

    # 1ª chamada: deve inserir 1 registro
    res_first = client.post("/api/v1/telemetry/events", json=payload)
    assert res_first.status_code == 201
    data_first = res_first.json()
    assert data_first["inserted_count"] == 1
    assert data_first["ignored_duplicates_in_db"] == 0

    # 2ª chamada (retry de rede com mesmo event_id): deve ignorar duplicata (inserted_count == 0)
    res_second = client.post("/api/v1/telemetry/events", json=payload)
    assert res_second.status_code == 201
    data_second = res_second.json()
    assert data_second["inserted_count"] == 0
    assert data_second["ignored_duplicates_in_db"] == 1

    # Verifica no banco SQL que há exatamente 1 linha e zero exposição de PII
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_telemetry_events")).scalar_one()
        assert count == 1
        row = conn.execute(
            text("SELECT user_hash, sanitized_metadata FROM fact_telemetry_events")
        ).mappings().one()
        assert "gabriel.santos@gamer.com" not in row["user_hash"]
        assert "gabriel.santos@gamer.com" not in row["sanitized_metadata"]
        assert "111.222.333-44" not in row["sanitized_metadata"]
        assert "Gabriel Santos" not in row["sanitized_metadata"]


def test_invalid_contract_returns_http_422(test_client):
    """Contratos inválidos (ex: categoria inexistente ou latência negativa) devem retornar HTTP 422."""
    client, _ = test_client
    bad_payload = {
        "event_id": "evt_bad_01",
        "tenant_id": "tenant_x",
        "product_id": "prod_x",
        "category": "INVALID_CATEGORY",
        "user_identifier": "user@test.com",
        "event_type": "API_CALL",
        "latency_ms": -50.0,
    }
    response = client.post("/api/v1/telemetry/events", json=bad_payload)
    assert response.status_code == 422


def test_batch_2000_synthetic_events_and_analytical_sql_queries(test_client):
    """
    Valida a meta de engenharia ponta a ponta:
    Gera 2.000 eventos sintéticos mistos (Games + SaaS) com sujeira intencional e duplicatas,
    executa o pipeline ETL e valida as queries analíticas OLAP via endpoint REST.
    """
    client, engine = test_client
    raw_events = generate_mock_events(total_events=2000, seed=99)
    assert len(raw_events) == 2000

    etl_result = transform_events_batch(raw_events)
    assert etl_result.received_count == 2000
    assert etl_result.deduplicated_in_batch == 100  # 5% de duplicatas no gerador
    assert len(etl_result.fact_events_df) == 1900

    inserted, ignored = persist_etl_batch(etl_result, engine=engine)
    assert inserted == 1900
    assert ignored == 0

    # Reexecutar o mesmo lote deve resultar em 0 novas inserções (100% idempotente)
    inserted_retry, ignored_retry = persist_etl_batch(etl_result, engine=engine)
    assert inserted_retry == 0
    assert ignored_retry == 1900

    # Consulta o endpoint de Analytics OLAP
    res_analytics = client.get("/api/v1/analytics/summary")
    assert res_analytics.status_code == 200
    summary = res_analytics.json()

    assert summary["total_events"] == 1900
    assert summary["total_unique_users"] > 100
    assert len(summary["dau_mau_metrics"]) >= 2
    assert len(summary["latency_p95_metrics"]) == 4
    assert len(summary["revenue_metrics"]) >= 2
    assert len(summary["retention_metrics"]) == 2
