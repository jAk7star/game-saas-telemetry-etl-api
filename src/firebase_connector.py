"""
Conector e Simulador Dual-Mode do Firebase Cloud Firestore para o Pipeline ETL.

Como o Firebase (Cloud Firestore) funciona nesta arquitetura:
1. O Firestore é um banco NoSQL orientado a documentos organizado em Coleções -> Documentos (identificados por `doc.id`).
2. Clientes de Jogos (Unity/Unreal/Mobile) e SaaS (Web/React) gravam eventos com suporte offline e sincronização em tempo real em uma coleção de staging (ex: `telemetry_raw_events`).
3. Os documentos no Firestore frequentemente possuem estrutura aninhada (ex: `user`, `metrics`, `context`, `product`, `tenant`).
4. Este módulo provê:
   - `normalize_firestore_document(doc_id, data)`: Achata (flattens) documentos aninhados ou planos do Firestore para o contrato compatível com `TelemetryEventInput` e a camada Zero-Exposure.
   - `InMemoryFirestoreStore`: Simulador fiel de `CollectionReference` e `DocumentSnapshot` do Firestore para testes locais, Swagger UI e CI/CD sem exigir credenciais do Google Cloud.
   - `FirestoreETLConnector`: Conector Dual-Mode que usa `firebase-admin` quando `FIREBASE_SERVICE_ACCOUNT_PATH` ou `FIRESTORE_EMULATOR_HOST` está configurado, ou opera sobre o simulador In-Memory por padrão, gerenciando o ciclo de vida pós-extração (`read_only`, `mark_synced_and_strip_pii`, `delete_after_ingest`).
"""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class FirestoreAckMode(str, Enum):
    """Política de ciclo de vida do documento no Cloud Firestore após ingestão no Data Warehouse SQL."""

    READ_ONLY = "read_only"
    MARK_SYNCED_AND_STRIP_PII = "mark_synced_and_strip_pii"
    DELETE_AFTER_INGEST = "delete_after_ingest"


@dataclass
class SimulatedDocumentSnapshot:
    """Representa um DocumentSnapshot do Cloud Firestore (`doc.id`, `doc.to_dict()`)."""

    id: str
    _data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._data)


class InMemoryFirestoreStore:
    """
    Simulador em memória do Cloud Firestore (Coleções -> Documentos).
    Permite demonstrar e testar a extração NoSQL -> Zero-Exposure -> Star Schema SQL
    sem depender de credenciais externas do GCP.
    """

    def __init__(self) -> None:
        self._collections: dict[str, dict[str, dict[str, Any]]] = {}

    def seed_default_collection(self, collection_name: str = "telemetry_raw_events") -> int:
        """
        Popula a coleção com documentos realistas aninhados (estilo Firebase/GA4)
        e documentos planos contendo PII (e-mail, CPF, IP, nome) para demonstrar a higienização.
        """
        sample_docs: dict[str, dict[str, Any]] = {
            "fs_doc_game_match_001": {
                "event_type": "MATCH_END",
                "occurred_at": "2026-11-15T14:20:00Z",
                "tenant": {
                    "id": "tenant_valkyrie_studios",
                    "name": "Valkyrie Studios",
                },
                "product": {
                    "id": "prod_cyber_arena_br",
                    "name": "Cyber Arena BR",
                    "category": "GAME",
                },
                "user": {
                    "email": "lucas.ferreira@gamer.com.br",
                    "name": "Lucas Ferreira",
                    "cpf": "123.456.789-01",
                    "tier": "PRO",
                },
                "context": {
                    "ip_address": "177.84.12.99",
                    "raw_metadata": "Match ranked finalizada player=Lucas Ferreira email=lucas.ferreira@gamer.com.br ip=177.84.12.99",
                },
                "metrics": {
                    "session_duration_sec": 1840,
                    "latency_ms": 28.5,
                    "revenue_brl": 0.0,
                },
                "etl_synced": False,
            },
            "fs_doc_game_purchase_002": {
                "event_type": "PURCHASE",
                "occurred_at": "2026-11-15T15:05:10Z",
                "tenant": {
                    "id": "tenant_valkyrie_studios",
                    "name": "Valkyrie Studios",
                },
                "product": {
                    "id": "prod_cyber_arena_br",
                    "name": "Cyber Arena BR",
                    "category": "GAME",
                },
                "user": {
                    "email": "lucas.ferreira@gamer.com.br",
                    "name": "Lucas Ferreira",
                    "cpf": "123.456.789-01",
                    "tier": "PRO",
                },
                "context": {
                    "ip_address": "177.84.12.99",
                    "raw_metadata": "BattlePass checkout cpf=123.456.789-01 user=Lucas Ferreira",
                },
                "metrics": {
                    "session_duration_sec": 420,
                    "latency_ms": 41.2,
                    "revenue_brl": 79.90,
                },
                "etl_synced": False,
            },
            "fs_doc_saas_api_003": {
                "event_type": "API_CALL",
                "occurred_at": "2026-11-16T09:30:00Z",
                "tenant": {
                    "id": "tenant_nexus_cloud",
                    "name": "Nexus Cloud Enterprise",
                },
                "product": {
                    "id": "prod_nexus_bi_suite",
                    "name": "Nexus BI Suite",
                    "category": "SAAS_ENTERPRISE",
                },
                "user": {
                    "identifier": "marina.costa@nexuscorp.io",
                    "name": "Marina Costa",
                    "cpf": "987.654.321-99",
                    "tier": "ENTERPRISE",
                },
                "context": {
                    "ip_address": "200.155.10.44",
                    "raw_metadata": "Export report request by Marina Costa (marina.costa@nexuscorp.io)",
                },
                "metrics": {
                    "session_duration_sec": 3600,
                    "latency_ms": 112.8,
                    "revenue_brl": 0.0,
                },
                "etl_synced": False,
            },
            "fs_doc_saas_purchase_004": {
                # Documento em formato plano (Flat) misturado na mesma coleção NoSQL
                "tenant_id": "tenant_nexus_cloud",
                "tenant_name": "Nexus Cloud Enterprise",
                "product_id": "prod_nexus_bi_suite",
                "product_name": "Nexus BI Suite",
                "category": "SAAS_ENTERPRISE",
                "user_identifier": "marina.costa@nexuscorp.io",
                "user_name": "Marina Costa",
                "user_cpf": "987.654.321-99",
                "ip_address": "200.155.10.44",
                "tier": "ENTERPRISE",
                "event_type": "PURCHASE",
                "session_duration_sec": 900,
                "latency_ms": 64.0,
                "revenue_brl": 1499.00,
                "raw_metadata": "Enterprise seat upgrade Marina Costa cpf 987.654.321-99",
                "occurred_at": "2026-11-16T10:15:00Z",
                "etl_synced": False,
            },
        }

        col = self._collections.setdefault(collection_name, {})
        for doc_id, payload in sample_docs.items():
            col[doc_id] = dict(payload)
        return len(sample_docs)

    def upsert_document(self, collection_name: str, doc_id: str, data: dict[str, Any]) -> None:
        col = self._collections.setdefault(collection_name, {})
        col[doc_id] = dict(data)

    def list_documents(
        self,
        collection_name: str,
        limit: int = 500,
        only_unsynced: bool = True,
    ) -> list[SimulatedDocumentSnapshot]:
        col = self._collections.get(collection_name, {})
        snapshots: list[SimulatedDocumentSnapshot] = []
        for doc_id, doc_data in col.items():
            if only_unsynced and doc_data.get("etl_synced") is True:
                continue
            snapshots.append(SimulatedDocumentSnapshot(id=doc_id, _data=dict(doc_data)))
            if len(snapshots) >= limit:
                break
        return snapshots

    def mark_synced_and_strip_pii(self, collection_name: str, doc_id: str) -> None:
        """
        Marca o documento como sincronizado (`etl_synced=True`) e expurga campos PII brutos
        na origem NoSQL (Firestore) para conformidade LGPD Zero-Exposure ponta a ponta.
        """
        col = self._collections.get(collection_name, {})
        if doc_id not in col:
            return
        doc = dict(col[doc_id])
        doc["etl_synced"] = True
        doc["etl_synced_at"] = datetime.now(timezone.utc).isoformat()

        # Remove PII no nível raiz
        for pii_field in ("user_identifier", "user_name", "user_cpf", "ip_address", "raw_metadata"):
            if pii_field in doc:
                doc[pii_field] = "[REDACTED_AFTER_ETL_SYNC]"

        # Remove PII em sub-mapas aninhados (`user` e `context`)
        if isinstance(doc.get("user"), dict):
            user_map = dict(doc["user"])
            for k in ("email", "identifier", "name", "cpf", "uid"):
                if k in user_map:
                    user_map[k] = "[REDACTED_AFTER_ETL_SYNC]"
            doc["user"] = user_map

        if isinstance(doc.get("context"), dict):
            ctx_map = dict(doc["context"])
            for k in ("ip_address", "ip", "raw_metadata"):
                if k in ctx_map:
                    ctx_map[k] = "[REDACTED_AFTER_ETL_SYNC]"
            doc["context"] = ctx_map

        col[doc_id] = doc

    def delete_document(self, collection_name: str, doc_id: str) -> None:
        col = self._collections.get(collection_name, {})
        col.pop(doc_id, None)

    def get_document(self, collection_name: str, doc_id: str) -> dict[str, Any] | None:
        col = self._collections.get(collection_name, {})
        if doc_id in col:
            return dict(col[doc_id])
        return None

    def count_documents(self, collection_name: str) -> int:
        return len(self._collections.get(collection_name, {}))


def normalize_firestore_document(doc_id: str, raw_doc: dict[str, Any]) -> dict[str, Any]:
    """
    Normaliza (flattens) um documento do Cloud Firestore (seja ele aninhado estilo Firebase/GA4
    ou plano) para o formato esperado pelo `TelemetryEventInput` / `transform_events_batch`.

    Regras de mapeamento:
    - `event_id`: usa `raw_doc.get("event_id")` ou o próprio `doc.id` determinístico do Firestore.
    - `tenant`: extrai de `raw_doc["tenant"]["id" | "name"]` ou `tenant_id` / `tenant_name`.
    - `product`: extrai de `raw_doc["product"]["id" | "name" | "category"]` ou campos planos.
    - `user`: extrai de `raw_doc["user"]["email" | "identifier" | "uid" | "name" | "cpf" | "tier"]`.
    - `context`: extrai de `raw_doc["context"]["ip_address" | "raw_metadata"]`.
    - `metrics`: extrai de `raw_doc["metrics"]["session_duration_sec" | "latency_ms" | "revenue_brl"]`.
    """
    tenant_map = raw_doc.get("tenant") if isinstance(raw_doc.get("tenant"), dict) else {}
    product_map = raw_doc.get("product") if isinstance(raw_doc.get("product"), dict) else {}
    user_map = raw_doc.get("user") if isinstance(raw_doc.get("user"), dict) else {}
    context_map = raw_doc.get("context") if isinstance(raw_doc.get("context"), dict) else {}
    metrics_map = raw_doc.get("metrics") if isinstance(raw_doc.get("metrics"), dict) else {}

    event_id = str(raw_doc.get("event_id") or doc_id).strip()
    tenant_id = str(
        raw_doc.get("tenant_id") or tenant_map.get("id") or "tenant_firebase_default"
    ).strip()
    tenant_name = str(
        raw_doc.get("tenant_name") or tenant_map.get("name") or "Firebase Default Tenant"
    ).strip()

    product_id = str(
        raw_doc.get("product_id") or product_map.get("id") or "prod_firebase_app"
    ).strip()
    product_name = str(
        raw_doc.get("product_name") or product_map.get("name") or "Firebase App"
    ).strip()
    category = str(raw_doc.get("category") or product_map.get("category") or "GAME").strip().upper()

    user_identifier = str(
        raw_doc.get("user_identifier")
        or user_map.get("identifier")
        or user_map.get("email")
        or user_map.get("uid")
        or "anonymous_firebase_user"
    ).strip()

    user_name = raw_doc.get("user_name") or user_map.get("name")
    user_cpf = raw_doc.get("user_cpf") or user_map.get("cpf")
    tier = str(raw_doc.get("tier") or user_map.get("tier") or "FREE").strip().upper()

    ip_address = raw_doc.get("ip_address") or context_map.get("ip_address") or context_map.get("ip")
    raw_metadata = raw_doc.get("raw_metadata") or context_map.get("raw_metadata")

    session_duration_sec = (
        raw_doc.get("session_duration_sec")
        if "session_duration_sec" in raw_doc
        else metrics_map.get("session_duration_sec", 0)
    )
    latency_ms = (
        raw_doc.get("latency_ms") if "latency_ms" in raw_doc else metrics_map.get("latency_ms", 0.0)
    )
    revenue_brl = (
        raw_doc.get("revenue_brl")
        if "revenue_brl" in raw_doc
        else metrics_map.get("revenue_brl", 0.0)
    )

    event_type = str(raw_doc.get("event_type") or "API_CALL").strip().upper()
    occurred_at = raw_doc.get("occurred_at") or datetime.now(timezone.utc).isoformat()

    return {
        "event_id": event_id,
        "tenant_id": tenant_id,
        "tenant_name": tenant_name,
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "user_identifier": user_identifier,
        "user_name": user_name,
        "user_cpf": user_cpf,
        "ip_address": ip_address,
        "tier": tier,
        "event_type": event_type,
        "session_duration_sec": session_duration_sec,
        "latency_ms": latency_ms,
        "revenue_brl": revenue_brl,
        "raw_metadata": raw_metadata,
        "occurred_at": occurred_at,
    }


class FirestoreETLConnector:
    """
    Conector Dual-Mode para extração de documentos do Cloud Firestore:
    - Modo `cloud_sdk`: utiliza `firebase_admin.firestore` quando `FIREBASE_SERVICE_ACCOUNT_PATH`
      ou `FIRESTORE_EMULATOR_HOST` está configurado no ambiente.
    - Modo `in_memory_simulator`: utiliza `InMemoryFirestoreStore` para execução local imediata,
      demonstrações no Swagger UI e testes automatizados (`pytest`).
    """

    def __init__(self, memory_store: InMemoryFirestoreStore | None = None) -> None:
        self.memory_store = memory_store or InMemoryFirestoreStore()
        if self.memory_store.count_documents("telemetry_raw_events") == 0:
            self.memory_store.seed_default_collection("telemetry_raw_events")

    @property
    def backend_mode(self) -> str:
        if os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH") or os.getenv("FIRESTORE_EMULATOR_HOST"):
            return "firebase_admin_sdk"
        return "in_memory_simulator"

    def _get_cloud_firestore_client(self):
        """Inicializa o cliente oficial do Firebase Admin SDK sob demanda."""
        import firebase_admin
        from firebase_admin import credentials, firestore

        if not firebase_admin._apps:
            sa_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
            if sa_path and os.path.exists(sa_path):
                cred = credentials.Certificate(sa_path)
                firebase_admin.initialize_app(cred)
            else:
                firebase_admin.initialize_app()
        return firestore.client()

    def extract_and_acknowledge(
        self,
        collection_name: str = "telemetry_raw_events",
        limit: int = 500,
        only_unsynced: bool = True,
        ack_mode: FirestoreAckMode = FirestoreAckMode.MARK_SYNCED_AND_STRIP_PII,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Extrai documentos da coleção do Cloud Firestore, normaliza o schema NoSQL (aninhado ou plano)
        para o contrato do pipeline ETL e aplica a política de ciclo de vida (`ack_mode`).

        Retorna `(normalized_events, acknowledged_docs_count)`.
        """
        if self.backend_mode == "firebase_admin_sdk":
            client = self._get_cloud_firestore_client()
            col_ref = client.collection(collection_name)
            query = col_ref.limit(limit)
            docs = list(query.stream())

            normalized: list[dict[str, Any]] = []
            ack_count = 0
            for snap in docs:
                raw_data = snap.to_dict() or {}
                if only_unsynced and raw_data.get("etl_synced") is True:
                    continue
                normalized.append(normalize_firestore_document(snap.id, raw_data))
                if ack_mode == FirestoreAckMode.MARK_SYNCED_AND_STRIP_PII:
                    col_ref.document(snap.id).set(
                        {
                            "etl_synced": True,
                            "etl_synced_at": datetime.now(timezone.utc).isoformat(),
                            "user_identifier": "[REDACTED_AFTER_ETL_SYNC]",
                            "user_name": "[REDACTED_AFTER_ETL_SYNC]",
                            "user_cpf": "[REDACTED_AFTER_ETL_SYNC]",
                            "ip_address": "[REDACTED_AFTER_ETL_SYNC]",
                            "raw_metadata": "[REDACTED_AFTER_ETL_SYNC]",
                        },
                        merge=True,
                    )
                    ack_count += 1
                elif ack_mode == FirestoreAckMode.DELETE_AFTER_INGEST:
                    col_ref.document(snap.id).delete()
                    ack_count += 1
            return normalized, ack_count

        # Modo Simulador In-Memory (padrão local / testes)
        snapshots = self.memory_store.list_documents(
            collection_name=collection_name,
            limit=limit,
            only_unsynced=only_unsynced,
        )
        normalized_events: list[dict[str, Any]] = []
        ack_count = 0

        for snap in snapshots:
            normalized_events.append(normalize_firestore_document(snap.id, snap.to_dict()))
            if ack_mode == FirestoreAckMode.MARK_SYNCED_AND_STRIP_PII:
                self.memory_store.mark_synced_and_strip_pii(collection_name, snap.id)
                ack_count += 1
            elif ack_mode == FirestoreAckMode.DELETE_AFTER_INGEST:
                self.memory_store.delete_document(collection_name, snap.id)
                ack_count += 1

        return normalized_events, ack_count
