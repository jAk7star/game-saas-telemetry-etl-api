-- ============================================================================
-- 02_analytical_queries.sql
-- Consultas Analíticas SQL Puras (CTEs & Window Functions)
-- Métricas: DAU/MAU (Stickiness), Retenção Coorte, Latência P95 e Receita
-- ============================================================================

-- 1. DAU, MAU e Stickiness Ratio por Categoria de Produto (GAME vs SAAS_ENTERPRISE)
WITH daily_users AS (
    SELECT
        p.category,
        DATE(f.occurred_at) AS activity_date,
        STRFTIME('%Y-%m', f.occurred_at) AS activity_month,
        COUNT(DISTINCT f.user_hash) AS dau
    FROM fact_telemetry_events f
    INNER JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.category, DATE(f.occurred_at), STRFTIME('%Y-%m', f.occurred_at)
),
monthly_users AS (
    SELECT
        p.category,
        STRFTIME('%Y-%m', f.occurred_at) AS activity_month,
        COUNT(DISTINCT f.user_hash) AS mau
    FROM fact_telemetry_events f
    INNER JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.category, STRFTIME('%Y-%m', f.occurred_at)
)
SELECT
    d.category,
    d.activity_month,
    ROUND(AVG(d.dau), 2) AS avg_dau,
    m.mau,
    ROUND((AVG(d.dau) * 100.0) / NULLIF(m.mau, 0), 2) AS stickiness_pct
FROM daily_users d
INNER JOIN monthly_users m
    ON d.category = m.category AND d.activity_month = m.activity_month
GROUP BY d.category, d.activity_month, m.mau
ORDER BY d.activity_month DESC, d.category;


-- 2. Latência P95, Média e Taxa de Timeout (ERROR_TIMEOUT) por Produto
WITH ranked_latency AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        f.latency_ms,
        f.event_type,
        PERCENT_RANK() OVER (
            PARTITION BY p.product_id
            ORDER BY f.latency_ms ASC
        ) AS pct_rank
    FROM fact_telemetry_events f
    INNER JOIN dim_products p ON f.product_id = p.product_id
),
p95_per_product AS (
    SELECT
        product_id,
        MIN(latency_ms) AS p95_latency_ms
    FROM ranked_latency
    WHERE pct_rank >= 0.95
    GROUP BY product_id
)
SELECT
    r.product_id,
    r.product_name,
    r.category,
    COUNT(*) AS total_events,
    ROUND(AVG(r.latency_ms), 2) AS avg_latency_ms,
    ROUND(COALESCE(p95.p95_latency_ms, MAX(r.latency_ms)), 2) AS p95_latency_ms,
    ROUND(
        SUM(CASE WHEN r.event_type = 'ERROR_TIMEOUT' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS timeout_error_rate_pct
FROM ranked_latency r
LEFT JOIN p95_per_product p95 ON r.product_id = p95.product_id
GROUP BY r.product_id, r.product_name, r.category, p95.p95_latency_ms
ORDER BY p95_latency_ms DESC;


-- 3. Receita Consolidada (BRL), ARPU e ARPPU por Categoria e Tier de Cliente
SELECT
    p.category,
    u.tier,
    COUNT(DISTINCT f.user_hash) AS active_users,
    COUNT(DISTINCT CASE WHEN f.event_type = 'PURCHASE' AND f.revenue_brl > 0 THEN f.user_hash END) AS paying_users,
    SUM(CASE WHEN f.event_type = 'PURCHASE' THEN 1 ELSE 0 END) AS total_purchases,
    ROUND(SUM(f.revenue_brl), 2) AS total_revenue_brl,
    ROUND(SUM(f.revenue_brl) / NULLIF(COUNT(DISTINCT f.user_hash), 0), 2) AS arpu_brl,
    ROUND(
        SUM(f.revenue_brl) / NULLIF(COUNT(DISTINCT CASE WHEN f.revenue_brl > 0 THEN f.user_hash END), 0),
        2
    ) AS arppu_brl
FROM fact_telemetry_events f
INNER JOIN dim_products p ON f.product_id = p.product_id
INNER JOIN dim_users u ON f.user_hash = u.user_hash
GROUP BY p.category, u.tier
ORDER BY p.category, total_revenue_brl DESC;


-- 4. Análise de Retenção e Engajamento de Sessão por Tenant (Multitenancy)
WITH user_activity_span AS (
    SELECT
        u.tenant_id,
        u.user_hash,
        u.tier,
        MIN(DATE(f.occurred_at)) AS first_event_date,
        MAX(DATE(f.occurred_at)) AS last_event_date,
        COUNT(DISTINCT DATE(f.occurred_at)) AS active_days,
        SUM(f.session_duration_sec) AS total_session_sec
    FROM dim_users u
    INNER JOIN fact_telemetry_events f ON u.user_hash = f.user_hash
    GROUP BY u.tenant_id, u.user_hash, u.tier
)
SELECT
    tenant_id,
    COUNT(user_hash) AS total_users,
    SUM(CASE WHEN active_days > 1 THEN 1 ELSE 0 END) AS retained_users,
    ROUND(SUM(CASE WHEN active_days > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(user_hash), 2) AS retention_rate_pct,
    ROUND(AVG(total_session_sec) / 60.0, 2) AS avg_engaged_minutes_per_user
FROM user_activity_span
GROUP BY tenant_id
ORDER BY retention_rate_pct DESC;
