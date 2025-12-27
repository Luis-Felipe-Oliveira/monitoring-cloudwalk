-- =====================================================
-- CloudWalk Monitoring System - SQL Queries
-- Gerado automaticamente em: 2025-12-27 12:45:43
-- =====================================================

-- =====================================================
-- PARTE 1: ANÁLISES DE CHECKOUT
-- =====================================================

-- 1. ESTATÍSTICAS GERAIS
-- Calcula métricas básicas dos checkouts
SELECT 
    COUNT(*) as total_records,
    AVG(today) as avg_today,
    AVG(yesterday) as avg_yesterday,
    AVG(avg_last_week) as avg_last_week,
    MAX(today) as max_today,
    MIN(today) as min_today
FROM checkouts;

-- 2. COMPARAÇÃO HOJE VS ONTEM
-- Identifica maiores variações percentuais
SELECT 
    time,
    today,
    yesterday,
    (today - yesterday) as difference,
    CASE 
        WHEN yesterday = 0 THEN NULL
        ELSE ROUND(((today - yesterday) * 100.0 / yesterday), 2)
    END as percent_change
FROM checkouts
WHERE yesterday > 0
ORDER BY ABS(today - yesterday) DESC;

-- 3. IDENTIFICAÇÃO DE ANOMALIAS
-- Vendas 50% acima da média semanal
SELECT 
    time,
    today,
    avg_last_week,
    (today - avg_last_week) as deviation,
    ROUND(((today - avg_last_week) * 100.0 / avg_last_week), 2) as percent_deviation
FROM checkouts
WHERE avg_last_week > 0 
    AND today > (avg_last_week * 1.5)
ORDER BY deviation DESC;

-- 4. ANÁLISE POR PERÍODO DO DIA
-- Agrupa vendas por período (Madrugada, Manhã, Tarde, Noite)
SELECT 
    CASE 
        WHEN CAST(REPLACE(time, 'h', '') AS INTEGER) BETWEEN 0 AND 5 THEN 'Madrugada (0-5h)'
        WHEN CAST(REPLACE(time, 'h', '') AS INTEGER) BETWEEN 6 AND 11 THEN 'Manhã (6-11h)'
        WHEN CAST(REPLACE(time, 'h', '') AS INTEGER) BETWEEN 12 AND 17 THEN 'Tarde (12-17h)'
        ELSE 'Noite (18-23h)'
    END as period,
    COUNT(*) as hours_count,
    ROUND(AVG(today), 2) as avg_today,
    ROUND(AVG(yesterday), 2) as avg_yesterday,
    SUM(today) as total_today
FROM checkouts
GROUP BY period
ORDER BY 
    CASE period
        WHEN 'Madrugada (0-5h)' THEN 1
        WHEN 'Manhã (6-11h)' THEN 2
        WHEN 'Tarde (12-17h)' THEN 3
        ELSE 4
    END;

-- 5. HORÁRIOS DE PICO E BAIXA
-- Identifica horários críticos
WITH stats AS (
    SELECT AVG(today) as mean FROM checkouts
)
SELECT 
    time,
    today,
    ROUND((SELECT mean FROM stats), 2) as overall_mean,
    CASE 
        WHEN today > (SELECT mean FROM stats) * 1.5 THEN 'PICO'
        WHEN today < (SELECT mean FROM stats) * 0.5 THEN 'BAIXO'
        ELSE 'NORMAL'
    END as classification
FROM checkouts
ORDER BY today DESC;

-- =====================================================
-- PARTE 2: ANÁLISES DE TRANSAÇÕES
-- =====================================================

-- 6. ESTATÍSTICAS POR STATUS
-- Volume e médias por status de transação
SELECT 
    status,
    COUNT(*) as records,
    SUM(count) as total_transactions,
    ROUND(AVG(count), 2) as avg_per_minute,
    MIN(count) as min_per_minute,
    MAX(count) as max_per_minute
FROM transactions
GROUP BY status
ORDER BY total_transactions DESC;

-- 7. EVOLUÇÃO TEMPORAL
-- Mostra acumulado de transações ao longo do tempo
SELECT 
    timestamp,
    status,
    count,
    SUM(count) OVER (PARTITION BY status ORDER BY timestamp) as cumulative_count
FROM transactions
ORDER BY timestamp, status;

-- 8. MINUTOS COM MAIOR VOLUME
-- Top minutos com mais transações
SELECT 
    timestamp,
    SUM(count) as total_transactions
FROM transactions
GROUP BY timestamp
ORDER BY total_transactions DESC
LIMIT 10;

-- 9. TAXA DE APROVAÇÃO
-- Percentual de transações aprovadas por minuto
WITH minute_stats AS (
    SELECT 
        timestamp,
        SUM(CASE WHEN UPPER(status) = 'APPROVED' THEN count ELSE 0 END) as approved,
        SUM(count) as total
    FROM transactions
    GROUP BY timestamp
)
SELECT 
    timestamp,
    approved,
    total,
    ROUND((approved * 100.0 / total), 2) as approval_rate
FROM minute_stats
WHERE total > 0
ORDER BY approval_rate DESC;

-- 10. ANÁLISE DE TENDÊNCIAS
-- Compara cada minuto com o anterior
SELECT 
    t1.timestamp,
    t1.status,
    t1.count as current_count,
    LAG(t1.count) OVER (PARTITION BY t1.status ORDER BY t1.timestamp) as previous_count,
    t1.count - LAG(t1.count) OVER (PARTITION BY t1.status ORDER BY t1.timestamp) as change
FROM transactions t1
ORDER BY t1.timestamp DESC, t1.status;

-- =====================================================
-- FIM DAS QUERIES
-- =====================================================
