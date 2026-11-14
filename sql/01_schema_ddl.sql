-- ============================================================================
-- 01_schema_ddl.sql
-- Modelagem Relacional Star Schema (OLAP) com Suporte a Multitenancy
-- Repositório: game-saas-telemetry-etl-api
-- ============================================================================

-- Dimensão de Tenants (Organizações / Studios Multitenant)
CREATE TABLE IF NOT EXISTS dim_tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    tenant_name VARCHAR(120) NOT NULL,
    segment VARCHAR(32) NOT NULL CHECK (segment IN ('GAME', 'SAAS_ENTERPRISE', 'HYBRID')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensão de Ecossistema / Produtos (Games & SaaS Enterprise)
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(120) NOT NULL,
    category VARCHAR(32) NOT NULL CHECK (category IN ('GAME', 'SAAS_ENTERPRISE')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensão de Jogadores / Clientes Anonimizados (Zero-Exposure LGPD)
-- Nenhum dado pessoal (e-mail, CPF, IP ou nome) é armazenado, apenas user_hash SHA-256
CREATE TABLE IF NOT EXISTS dim_users (
    user_hash CHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    tier VARCHAR(32) NOT NULL CHECK (tier IN ('FREE', 'PRO', 'ENTERPRISE')),
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id) REFERENCES dim_tenants(tenant_id)
);

-- Tabela Fato de Sessões e Métricas de Telemetria (Idempotente via PK event_id)
CREATE TABLE IF NOT EXISTS fact_telemetry_events (
    event_id VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL,
    user_hash CHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('MATCH_END', 'API_CALL', 'PURCHASE', 'ERROR_TIMEOUT')),
    session_duration_sec INTEGER NOT NULL DEFAULT 0 CHECK (session_duration_sec >= 0),
    latency_ms FLOAT NOT NULL DEFAULT 0.0 CHECK (latency_ms >= 0.0),
    revenue_brl NUMERIC(12, 2) NOT NULL DEFAULT 0.00 CHECK (revenue_brl >= 0.00),
    sanitized_metadata TEXT,
    occurred_at TIMESTAMP NOT NULL,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (user_hash) REFERENCES dim_users(user_hash)
);

-- Índices para Aceleração de Consultas Analíticas (OLAP)
CREATE INDEX IF NOT EXISTS idx_fact_occurred_at ON fact_telemetry_events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_fact_product_event ON fact_telemetry_events(product_id, event_type);
CREATE INDEX IF NOT EXISTS idx_fact_user_hash ON fact_telemetry_events(user_hash);
CREATE INDEX IF NOT EXISTS idx_users_tenant_tier ON dim_users(tenant_id, tier);
