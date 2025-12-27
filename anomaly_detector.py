import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List
import json

class AnomalyDetector:
    """
    Sistema de detecção de anomalias em transações
    Corrigido para trabalhar com dados agregados (timestamp, status, count)
    """
    
    def __init__(self, transactions_path: str, auth_codes_path: str = None):
        """Inicializa o detector com dados históricos"""
        print("Inicializando Anomaly Detector...")
        
        # Carregar dados
        self.df_trans = pd.read_csv(transactions_path)
        print(f"✓ Transações carregadas: {len(self.df_trans)}")
        
        if auth_codes_path:
            self.df_auth = pd.read_csv(auth_codes_path)
            print(f"✓ Auth codes carregados: {len(self.df_auth)}")
        else:
            self.df_auth = None
        
        # Preparar dados
        self._prepare_data()
        
        # Calcular baseline
        self.baseline = self._calculate_baseline()
        
        # Configurar thresholds
        self.thresholds = self._configure_thresholds()
        
        print("✓ Detector inicializado!")
    
    def _prepare_data(self):
        """Prepara e limpa os dados"""
        # Converter timestamp
        if 'timestamp' in self.df_trans.columns:
            self.df_trans['timestamp'] = pd.to_datetime(self.df_trans['timestamp'])
            self.df_trans['minute'] = self.df_trans['timestamp'].dt.floor('min')
        
        # Normalizar status para uppercase
        if 'status' in self.df_trans.columns:
            self.df_trans['status'] = self.df_trans['status'].str.upper()
        
        print("\nStatus únicos encontrados:", self.df_trans['status'].unique().tolist())
    
    def _calculate_baseline(self) -> Dict:
        """Calcula métricas baseline do histórico"""
        print("\nCalculando baseline histórico...")
        
        baseline = {}
        
        # Para cada status único
        for status in self.df_trans['status'].unique():
            status_data = self.df_trans[self.df_trans['status'] == status]['count']
            
            if len(status_data) > 0:
                baseline[status] = {
                    'mean': status_data.mean(),
                    'std': status_data.std(),
                    'median': status_data.median(),
                    'p95': status_data.quantile(0.95),
                    'p99': status_data.quantile(0.99),
                    'max': status_data.max(),
                    'min': status_data.min()
                }
                
                print(f"  {status}: mean={baseline[status]['mean']:.2f}, "
                      f"std={baseline[status]['std']:.2f}, "
                      f"p95={baseline[status]['p95']:.2f}, "
                      f"p99={baseline[status]['p99']:.2f}")
        
        return baseline
    
    def _configure_thresholds(self) -> Dict:
        """Configura thresholds para alertas"""
        thresholds = {}
        
        # Status críticos que queremos monitorar
        critical_statuses = ['FAILED', 'DENIED', 'REVERSED', 'REJECTED']
        
        for status in critical_statuses:
            if status in self.baseline:
                # Usar percentis como thresholds
                thresholds[status] = {
                    'warning': self.baseline[status]['p95'],
                    'critical': self.baseline[status]['p99'],
                    'method': 'percentile'
                }
            else:
                # Se não temos histórico, usar valores padrão conservadores
                thresholds[status] = {
                    'warning': 10,  # 10 transações por minuto
                    'critical': 20,  # 20 transações por minuto
                    'method': 'default'
                }
        
        print("\nThresholds configurados:")
        for status, values in thresholds.items():
            print(f"  {status}: Warning={values['warning']:.2f}, "
                  f"Critical={values['critical']:.2f} ({values['method']})")
        
        return thresholds
    
    def analyze_transaction_window(self, transactions: List[Dict]) -> Dict:
        """
        Analisa uma janela de transações agregadas
        
        Args:
            transactions: Lista com formato [{"status": "APPROVED", "count": 120}, ...]
        
        Returns:
            Dict com análise e recomendação
        """
        if not transactions:
            return {
                'alert': False,
                'severity': 'NORMAL',
                'message': 'Sem transações para analisar',
                'anomaly_score': 0
            }
        
        # Agregar transações por status
        status_counts = {}
        for trans in transactions:
            status = trans.get('status', 'UNKNOWN').upper()
            count = trans.get('count', 1)
            status_counts[status] = status_counts.get(status, 0) + count
        
        # Analisar cada status crítico
        alerts = []
        max_severity = 'NORMAL'
        anomaly_score = 0
        
        critical_statuses = ['FAILED', 'DENIED', 'REVERSED', 'REJECTED']
        
        for status in critical_statuses:
            count = status_counts.get(status, 0)
            
            if status in self.thresholds:
                warning_threshold = self.thresholds[status]['warning']
                critical_threshold = self.thresholds[status]['critical']
                
                if count >= critical_threshold:
                    alerts.append({
                        'status': status,
                        'count': count,
                        'severity': 'CRITICAL',
                        'threshold': critical_threshold,
                        'message': f'{status} critically high: {count} (threshold: {critical_threshold:.0f})'
                    })
                    max_severity = 'CRITICAL'
                    anomaly_score += 100
                    
                elif count >= warning_threshold:
                    alerts.append({
                        'status': status,
                        'count': count,
                        'severity': 'WARNING',
                        'threshold': warning_threshold,
                        'message': f'{status} above normal: {count} (threshold: {warning_threshold:.0f})'
                    })
                    if max_severity == 'NORMAL':
                        max_severity = 'WARNING'
                    anomaly_score += 50
        
        # Limitar score a 100
        anomaly_score = min(anomaly_score, 100)
        
        result = {
            'alert': len(alerts) > 0,
            'severity': max_severity,
            'anomaly_score': anomaly_score,
            'status_counts': status_counts,
            'alerts': alerts,
            'timestamp': datetime.now().isoformat(),
            'total_transactions': sum(status_counts.values())
        }
        
        if alerts:
            result['message'] = f"⚠️  {len(alerts)} anomaly(ies) detected!"
        else:
            result['message'] = "✓ All transactions within normal range"
        
        return result
    
    def analyze_real_time(self, transaction: Dict) -> Dict:
        """
        Analisa uma transação individual ou agregada
        
        Args:
            transaction: {"status": "APPROVED", "count": 120} ou {"status": "FAILED"}
        
        Returns:
            Dict com análise
        """
        status = transaction.get('status', 'UNKNOWN').upper()
        count = transaction.get('count', 1)
        
        # Verificar se é status crítico
        critical_statuses = ['FAILED', 'DENIED', 'REVERSED', 'REJECTED']
        rule_based_alert = status in critical_statuses
        
        # Verificar se count está acima do baseline
        if status in self.baseline:
            mean = self.baseline[status]['mean']
            if count > mean * 2:  # 2x acima da média
                rule_based_alert = True
        
        return {
            'status': status,
            'count': count,
            'alert': rule_based_alert,
            'reason': f'Status: {status}, Count: {count}',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_statistics(self) -> Dict:
        """Retorna estatísticas do detector"""
        return {
            'baseline': {k: {ki: float(vi) if isinstance(vi, (np.int64, np.float64)) else vi 
                             for ki, vi in v.items()} 
                        for k, v in self.baseline.items()},
            'thresholds': self.thresholds,
            'total_transactions_analyzed': len(self.df_trans),
            'unique_statuses': self.df_trans['status'].unique().tolist()
        }

# Teste
if __name__ == "__main__":
    print("="*60)
    print("TESTE DO ANOMALY DETECTOR")
    print("="*60)
    
    # Inicializar detector
    detector = AnomalyDetector('data/transactions.csv', 'data/transactions_auth_codes.csv')
    
    # Teste 1: Transações normais
    print("\n" + "="*60)
    print("TESTE 1: Transações Normais")
    print("="*60)
    
    normal_transactions = [
        {'status': 'approved', 'count': 120},
        {'status': 'approved', 'count': 115},
        {'status': 'approved', 'count': 110}
    ]
    
    result = detector.analyze_transaction_window(normal_transactions)
    print(json.dumps(result, indent=2, default=str))
    
    # Teste 2: Simulando anomalia
    print("\n" + "="*60)
    print("TESTE 2: Simulando Anomalia (muitas falhas)")
    print("="*60)
    
    anomaly_transactions = [
        {'status': 'approved', 'count': 50},
        {'status': 'failed', 'count': 30},  # Muito alto!
        {'status': 'denied', 'count': 20}   # Alto também!
    ]
    
    result = detector.analyze_transaction_window(anomaly_transactions)
    print(json.dumps(result, indent=2, default=str))
    
    # Estatísticas
    print("\n" + "="*60)
    print("ESTATÍSTICAS DO DETECTOR")
    print("="*60)
    stats = detector.get_statistics()
    print(json.dumps(stats, indent=2, default=str))