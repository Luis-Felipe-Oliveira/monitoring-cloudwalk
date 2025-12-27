import pandas as pd
import sqlite3
from datetime import datetime
import os

class SQLAnalyzer:
    def __init__(self, checkout_path=None, transactions_path=None):
        """Inicializa conexão SQLite e carrega dados"""
        self.conn = sqlite3.connect(':memory:')
        
        # Carregar checkout se fornecido
        if checkout_path and os.path.exists(checkout_path):
            self.df_checkout = pd.read_csv(checkout_path)
            self.df_checkout.to_sql('checkouts', self.conn, if_exists='replace', index=False)
            print(f"✓ Checkout carregado no SQLite: {len(self.df_checkout)} registros")
        else:
            self.df_checkout = None
            
        # Carregar transações se fornecido
        if transactions_path and os.path.exists(transactions_path):
            self.df_trans = pd.read_csv(transactions_path)
            self.df_trans.to_sql('transactions', self.conn, if_exists='replace', index=False)
            print(f"✓ Transações carregadas no SQLite: {len(self.df_trans)} registros")
        else:
            self.df_trans = None
        
    def execute_query(self, query, description):
        """Executa query e exibe resultados"""
        print("\n" + "="*60)
        print(f"QUERY: {description}")
        print("="*60)
        print(f"\nSQL:\n{query}\n")
        
        try:
            result = pd.read_sql_query(query, self.conn)
            print("Resultado:")
            print(result.to_string())
            print(f"\nTotal de linhas: {len(result)}")
            return result
        except Exception as e:
            print(f"ERRO: {e}")
            return None
    
    def run_checkout_analysis(self):
        """Executa análises nos dados de checkout"""
        if self.df_checkout is None:
            print("⚠️  Dados de checkout não carregados")
            return
        
        print("\n" + "="*60)
        print("ANÁLISES SQL - CHECKOUT")
        print("="*60)
        
        # Query 1: Estatísticas básicas
        query1 = """
        SELECT 
            COUNT(*) as total_records,
            AVG(today) as avg_today,
            AVG(yesterday) as avg_yesterday,
            AVG(avg_last_week) as avg_last_week,
            MAX(today) as max_today,
            MIN(today) as min_today
        FROM checkouts
        """
        self.execute_query(query1, "Estatísticas Gerais")
        
        # Query 2: Comparação hoje vs ontem
        query2 = """
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
        ORDER BY ABS(today - yesterday) DESC
        LIMIT 10
        """
        self.execute_query(query2, "Top 10 Maiores Variações (Hoje vs Ontem)")
        
        # Query 3: Identificar anomalias (>150% da média)
        query3 = """
        SELECT 
            time,
            today,
            avg_last_week,
            (today - avg_last_week) as deviation,
            ROUND(((today - avg_last_week) * 100.0 / avg_last_week), 2) as percent_deviation
        FROM checkouts
        WHERE avg_last_week > 0 
            AND today > (avg_last_week * 1.5)
        ORDER BY deviation DESC
        """
        self.execute_query(query3, "Anomalias - Vendas >150% da Média Semanal")
        
        # Query 4: Análise por faixa de hora
        query4 = """
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
            SUM(today) as total_today,
            SUM(yesterday) as total_yesterday
        FROM checkouts
        GROUP BY period
        ORDER BY 
            CASE period
                WHEN 'Madrugada (0-5h)' THEN 1
                WHEN 'Manhã (6-11h)' THEN 2
                WHEN 'Tarde (12-17h)' THEN 3
                ELSE 4
            END
        """
        self.execute_query(query4, "Análise por Período do Dia")
        
        # Query 5: Horários críticos (muito acima ou abaixo da média)
        query5 = """
        WITH stats AS (
            SELECT AVG(today) as mean, 
                   (MAX(today) - MIN(today)) as range_val
            FROM checkouts
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
        WHERE today > (SELECT mean FROM stats) * 1.5
            OR today < (SELECT mean FROM stats) * 0.5
        ORDER BY today DESC
        """
        self.execute_query(query5, "Horários Críticos (Pico ou Baixo)")
        
    def run_transactions_analysis(self):
        """Executa análises nos dados de transações"""
        if self.df_trans is None:
            print("⚠️  Dados de transações não carregados")
            return
        
        print("\n" + "="*60)
        print("ANÁLISES SQL - TRANSAÇÕES")
        print("="*60)
        
        # Query 1: Estatísticas por status
        query1 = """
        SELECT 
            status,
            COUNT(*) as records,
            SUM(count) as total_transactions,
            ROUND(AVG(count), 2) as avg_per_minute,
            MIN(count) as min_per_minute,
            MAX(count) as max_per_minute
        FROM transactions
        GROUP BY status
        ORDER BY total_transactions DESC
        """
        self.execute_query(query1, "Estatísticas por Status")
        
        # Query 2: Evolução temporal
        query2 = """
        SELECT 
            timestamp,
            status,
            count,
            SUM(count) OVER (PARTITION BY status ORDER BY timestamp) as cumulative_count
        FROM transactions
        ORDER BY timestamp, status
        LIMIT 20
        """
        self.execute_query(query2, "Evolução Temporal (Primeiros 20)")
        
        # Query 3: Minutos com maior volume
        query3 = """
        SELECT 
            timestamp,
            SUM(count) as total_transactions,
            GROUP_CONCAT(status || ':' || count) as breakdown
        FROM transactions
        GROUP BY timestamp
        ORDER BY total_transactions DESC
        LIMIT 10
        """
        self.execute_query(query3, "Top 10 Minutos com Maior Volume")
        
        # Query 4: Taxa de aprovação por minuto
        query4 = """
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
        ORDER BY approval_rate ASC
        LIMIT 10
        """
        self.execute_query(query4, "Top 10 Minutos com Menor Taxa de Aprovação")
    
    def save_queries_to_file(self):
        """Salva queries em arquivo SQL"""
        os.makedirs('queries', exist_ok=True)
        
        queries = """-- =====================================================
-- CloudWalk Monitoring System - SQL Queries
-- Gerado automaticamente em: {timestamp}
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
""".format(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        with open('queries/sql_queries.sql', 'w', encoding='utf-8') as f:
            f.write(queries)
        
        print("\n✓ Queries salvas em: queries/sql_queries.sql")
    
    def close(self):
        """Fecha conexão"""
        self.conn.close()

# Executar
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SQL ANALYZER - CloudWalk Monitoring")
    print("="*60)
    
    # Criar analyzer
    analyzer = SQLAnalyzer(
        checkout_path='data/checkout_1.csv',
        transactions_path='data/transactions.csv'
    )
    
    # Executar análises
    analyzer.run_checkout_analysis()
    analyzer.run_transactions_analysis()
    
    # Salvar queries
    analyzer.save_queries_to_file()
    
    # Fechar conexão
    analyzer.close()
    
    print("\n" + "="*60)
    print("✅ ANÁLISE SQL CONCLUÍDA!")
    print("="*60)
    print("\n📁 Arquivos gerados:")
    print("   - queries/sql_queries.sql")
    print("\n💡 Todas as queries podem ser executadas em qualquer banco SQL")