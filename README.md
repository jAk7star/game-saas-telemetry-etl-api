# 🎮 Game & SaaS Telemetry ETL Engine

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI%20%7C%20OpenAPI%203.0-009688.svg)](https://fastapi.tiangolo.com/)
[![SQL Star Schema](https://img.shields.io/badge/Warehouse-SQL%20Star%20Schema-E38C00.svg)](sql/01_schema_ddl.sql)
[![Zero-Exposure Security](https://img.shields.io/badge/Security-Zero--Exposure%20%28GDPR%2FLGPD%29-critical.svg)](src/sanitizer.py)
[![Docker Compose](https://img.shields.io/badge/Deploy-Docker%20Compose-2496ED.svg)](docker-compose.yml)
[![Tests Passing](https://img.shields.io/badge/pytest-10%20passed-brightgreen.svg)](tests/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> High-throughput data ingestion pipeline, **Zero-Exposure PII sanitizer**, and **analytical Star Schema warehouse** for hybrid Gaming and B2B SaaS telemetry.

---

## 🎯 Executive Overview

Hybrid gaming ecosystems and multi-tenant SaaS platforms emit thousands of continuous telemetry events per second across game clients (Unity, Unreal, Mobile), web dashboards, and backend services.

Uncontrolled telemetry ingestion introduces severe operational risks:
1. **PII Contamination:** Accidental leakage of customer names, unmasked IP addresses, and emails into analytical datalakes violates GDPR and LGPD.
2. **Duplicate Metrics:** Network retries cause phantom user counts and corrupted revenue reporting.
3. **Analytical Lockup:** Flat JSON storage degrades queries calculating Daily Active Users (DAU), P95 round-trip latency, and retention cohorts.

This repository provides an enterprise-grade ETL API engineered with **FastAPI**, **Pandas**, **SQLAlchemy**, and a **Star Schema relational warehouse**. It enforces zero-exposure privacy protection at the ingestion perimeter, normalizes dual-source streams (REST and Cloud Firestore NoSQL), and guarantees idempotent warehouse persistence.

---

## 🏗️ Architecture Pipeline

```mermaid
flowchart TD
    subgraph Sources ["1. Event Sources & Edge Clients"]
        GameClient["Mobile / PC Game Client<br/>(Telemetry SDK)"]
        SaaSApp["B2B SaaS Web Application<br/>(Session & Audit Logs)"]
        NoSQL["Google Cloud Firestore<br/>(Raw Event Staging)"]
    end

    subgraph IngestionBoundary ["2. Ingestion Boundary & Schema Enforcement"]
        GameClient --> |"POST /api/v1/telemetry/events"| API["FastAPI Ingestion Engine<br/>(Pydantic v2 Contract Gate)"]
        SaaSApp --> |"POST /api/v1/telemetry/events"| API
        NoSQL --> |"POST /api/v1/telemetry/sync-firestore"| Extractor["Dual-Mode Firestore Connector<br/>(Recursive Map Flattener)"]
        Extractor --> API
        API --> |"Invalid Payload"| HTTP422["HTTP 422 Strict Reject"]
    end

    subgraph ZeroExposureLayer ["3. Zero-Exposure Sanitization Layer"]
        API --> Sanitizer["Zero-Exposure PII Sanitizer<br/>(Regex Masker + SHA-256 Hasher)"]
        Sanitizer --> MaskEmail["Scrub Email & Tax IDs"]
        Sanitizer --> MaskIP["Preserve Subnet (x.x.x.0/24)"]
        Sanitizer --> HashUser["Salted SHA-256 User Token"]
    end

    subgraph BatchETL ["4. Pandas ETL & Deduplication Engine"]
        MaskEmail & MaskIP & HashUser --> Pandas["Vectorized Batch Transformer"]
        Pandas --> Deduplicate["Deduplicate on event_id & timestamp"]
        Deduplicate --> TypeCast["Type Coercion & Schema Conformance"]
    end

    subgraph DataWarehouse ["5. Relational Star Schema Warehouse"]
        TypeCast --> DB[("SQL Relational Warehouse<br/>(PostgreSQL / SQLite)")]
        DB --> DimTenants["dim_tenants"]
        DB --> DimProducts["dim_products"]
        DB --> DimUsers["dim_users"]
        DB --> FactEvents["fact_telemetry_events"]
    end

    subgraph Analytics ["6. Executive & Operational Reporting"]
        FactEvents --> KPI1["DAU / MAU & Engagement Ratios"]
        FactEvents --> KPI2["P95 Network Latency by Region"]
        FactEvents --> KPI3["Microtransaction & SaaS ARR Tracking"]
        FactEvents --> KPI4["Day-1, Day-7, Day-30 Retention Cohorts"]
    end
```

---

## 🛡️ Zero-Exposure Privacy Engineering

To guarantee zero personal data leakage, raw telemetry payloads pass through `src/sanitizer.py` before touching disk or database:

- **Salted SHA-256 User Hashes:** Customer IDs, emails, and Brazilian CPFs are normalized and converted into deterministic surrogate keys (`usr_sha256_<64hex>`). The warehouse joins user journeys without ever persisting raw identities.
- **Subnet IP Masking:** IPv4 addresses have their final octet zeroed (`192.168.1.144` $\rightarrow$ `192.168.1.0/24`). Regional geolocation resolution remains accurate while individual tracking is prevented.
- **Regex PII Stripping:** Unstructured text fields (error messages, search queries, stack traces) are scanned and scrubbed of email patterns, formatted/unformatted tax IDs, and IP addresses.
- **Firestore Source Purge:** In Firestore pull mode, the connector supports `mark_synced_and_strip_pii`, replacing sensitive fields in the staging document with `[REDACTED_AFTER_ETL_SYNC]` immediately upon successful warehouse ingestion.

---

## 🗄️ Relational Star Schema Model

```mermaid
erDiagram
    dim_tenants ||--o{ fact_telemetry_events : owns
    dim_products ||--o{ fact_telemetry_events : produces
    dim_users ||--o{ fact_telemetry_events : triggers

    dim_tenants {
        string tenant_id PK
        string tenant_name
        string plan_tier
        timestamp created_at
    }

    dim_products {
        string product_id PK
        string product_name
        string product_type
        string current_version
    }

    dim_users {
        string user_hash PK
        string region_code
        string client_platform
        timestamp first_seen_at
    }

    fact_telemetry_events {
        string event_id PK
        string tenant_id FK
        string product_id FK
        string user_hash FK
        string event_type
        string session_id
        timestamp event_timestamp
        float latency_ms
        float transaction_amount_usd
        string ip_masked
        json payload_sanitized
    }
```

---

## 📈 Analytical Capabilities

The relational model directly answers core business questions via optimized SQL in `sql/02_analytical_queries.sql`:

1. **Engagement & Stickiness:** Calculates Daily Active Users (DAU), Monthly Active Users (MAU), and the DAU/MAU ratio across products.
2. **Quality of Service (QoS):** Computes **P95 latency** per region and client platform, isolating infrastructure degradations before they cause user churn.
3. **Monetization Analytics:** Aggregates gaming microtransactions and SaaS subscription events into daily gross merchandise value (GMV) by plan tier.
4. **Cohort Retention:** Tracks player and tenant retention at Day 1, Day 7, and Day 30 marks.

---

## 🚀 Quickstart & Execution

### Option 1: Run with Docker Compose (Recommended)
```bash
# Clone the repository
git clone https://github.com/jAk7star/game-saas-telemetry-etl-api.git
cd game-saas-telemetry-etl-api

# Launch the containerized API and PostgreSQL warehouse
docker compose up --build -d

# Verify API health
curl http://localhost:8000/health
```
Access interactive OpenAPI documentation at **`http://localhost:8000/docs`**.

### Option 2: Local Python Setup
```bash
# 1. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt
pip install pytest ruff

# 3. Run the automated test suite
pytest -v

# 4. Generate 2,000 synthetic events and run batch ETL
python scripts/generate_mock_events.py --count 2000
python -m uvicorn src.main:app --reload --port 8000
```

---

## 🧪 Automated Testing Suite

The repository contains end-to-end integration and unit tests in `tests/`:
- **`tests/test_sanitizer.py`**: Verifies deterministic SHA-256 user hashing, subnet masking, and multi-pattern PII scrubbing.
- **`tests/test_api_etl.py`**: Validates REST ingestion contracts, HTTP 422 validation guards, Firestore dual-mode sync, idempotency against duplicate event IDs, and 2,000-event batch SQL execution.

```bash
pytest -v
```

---

## 👨‍💻 Author

**Jaques Gabriel Amaral Nunes**  
Data & AI Engineer  
- GitHub: [@jAk7star](https://github.com/jAk7star)  
- Email: [jaquesgabrielamaral@gmail.com](mailto:jaquesgabrielamaral@gmail.com)
