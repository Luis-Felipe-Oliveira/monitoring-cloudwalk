import requests
import json
import time
from datetime import datetime
import random

API_URL = "http://localhost:5000"

def test_health():
    """Testa health check"""
    print("\n" + "="*60)
    print("TESTE 1: Health Check")
    print("="*60)
    
    try:
        response = requests.get(f"{API_URL}/health")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"ERRO: {e}")
        return False

def test_send_transaction(transaction):
    """Envia uma transação para a API"""
    response = requests.post(
        f"{API_URL}/transaction",
        json=transaction,
        headers={'Content-Type': 'application/json'}
    )
    return response

def test_single_transaction():
    """Testa envio de transação individual"""
    print("\n" + "="*60)
    print("TESTE 2: Enviar Transação Individual")
    print("="*60)
    
    transaction = {
        'status': 'approved',
        'count': 120,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"Enviando: {json.dumps(transaction, indent=2)}")
    response = test_send_transaction(transaction)
    
    print(f"\nStatus: {response.status_code}")
    if response.status_code == 200:
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    else:
        print(f"Error: {response.text}")
    
    return response.status_code == 200

def test_anomaly_detection():
    """Testa detecção de anomalias"""
    print("\n" + "="*60)
    print("TESTE 3: Detecção de Anomalias")
    print("="*60)
    print("Enviando transações para simular anomalia...\n")
    
    # Enviar transações normais
    print("Enviando 5 transações APPROVED normais...")
    for i in range(5):
        transaction = {
            'status': 'approved',
            'count': random.randint(100, 130)
        }
        response = test_send_transaction(transaction)
        print(f"  ✓ Transação {i+1}: APPROVED")
        time.sleep(0.1)
    
    # Simular anomalia com muitas falhas
    print("\nSimulando ANOMALIA: enviando muitas transações FAILED...")
    for i in range(10):
        transaction = {
            'status': 'failed',
            'count': random.randint(20, 40)  # Muito alto!
        }
        response = test_send_transaction(transaction)
        
        if response.status_code == 200:
            data = response.json()
            if data['window_analysis']['alert']:
                print(f"  ⚠️  Transação {i+1}: FAILED - ALERTA DETECTADO!")
                print(f"      Severity: {data['window_analysis']['severity']}")
                print(f"      Score: {data['window_analysis']['anomaly_score']}")
            else:
                print(f"  ✓ Transação {i+1}: FAILED - Ainda normal")
        time.sleep(0.2)
    
    print("\n✓ Teste de anomalias concluído!")

def test_get_alerts():
    """Testa endpoint de alertas"""
    print("\n" + "="*60)
    print("TESTE 4: Buscar Alertas")
    print("="*60)
    
    response = requests.get(f"{API_URL}/alerts")
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\nTotal de alertas: {data['total_alerts']}")
        if data['alerts']:
            print(f"\nÚltimos 3 alertas:")
            for alert in data['alerts'][-3:]:
                print(f"\n  ID: {alert['id']}")
                print(f"  Timestamp: {alert['timestamp']}")
                print(f"  Severity: {alert['severity']}")
                print(f"  Details: {alert['details']}")
    
    return response.status_code == 200

def test_dashboard():
    """Testa endpoint do dashboard"""
    print("\n" + "="*60)
    print("TESTE 5: Dashboard Data")
    print("="*60)
    
    response = requests.get(f"{API_URL}/dashboard")
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print("\n📊 STATUS ATUAL:")
        print(f"  Total de transações: {data['current_status']['total_transactions']}")
        print(f"  Taxa de erro: {data['current_status']['error_rate_percent']}%")
        print(f"  Distribuição:")
        for status, count in data['current_status']['status_distribution'].items():
            print(f"    - {status}: {count}")
        
        print(f"\n🚨 ALERTAS:")
        print(f"  Total: {data['alerts_count']['total']}")
        print(f"  Críticos: {data['alerts_count']['critical']}")
        print(f"  Warnings: {data['alerts_count']['warning']}")
    
    return response.status_code == 200

def run_simulation():
    """Simula carga real"""
    print("\n" + "="*60)
    print("TESTE 6: Simulação de Carga Real (30 segundos)")
    print("="*60)
    print("Enviando mix realista de transações...\n")
    
    start_time = time.time()
    transaction_count = 0
    
    # Mix realista: 90% aprovadas, 7% negadas, 3% falhas
    while time.time() - start_time < 30:
        rand = random.random()
        
        if rand < 0.90:
            status = 'approved'
            count = random.randint(100, 150)
        elif rand < 0.97:
            status = 'denied'
            count = random.randint(5, 15)
        else:
            status = 'failed'
            count = random.randint(2, 8)
        
        transaction = {
            'status': status,
            'count': count,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            response = test_send_transaction(transaction)
            if response.status_code == 200:
                transaction_count += 1
                
                data = response.json()
                if data['window_analysis']['alert']:
                    print(f"  ⚠️  ALERTA! Trans #{transaction_count}: {data['window_analysis']['message']}")
            else:
                print(f"  ✗ Erro: {response.status_code}")
        except Exception as e:
            print(f"  ✗ Erro: {str(e)}")
        
        time.sleep(0.5)
    
    print(f"\n✓ Simulação concluída! Total: {transaction_count} transações")

def run_all_tests():
    """Executa todos os testes"""
    print("\n" + "="*70)
    print(" " * 15 + "CLOUDWALK MONITORING API - TESTES")
    print("="*70)
    
    try:
        # Teste 1
        if not test_health():
            print("\n❌ API não está respondendo!")
            print("   Execute: python api.py")
            return
        
        time.sleep(1)
        
        # Teste 2
        test_single_transaction()
        time.sleep(1)
        
        # Teste 3
        test_anomaly_detection()
        time.sleep(1)
        
        # Teste 4
        test_get_alerts()
        time.sleep(1)
        
        # Teste 5
        test_dashboard()
        time.sleep(1)
        
        # Teste 6
        run_simulation()
        
        # Dashboard final
        print("\n" + "="*60)
        print("📊 DASHBOARD FINAL")
        print("="*60)
        test_dashboard()
        
        print("\n" + "="*70)
        print("✅ TODOS OS TESTES CONCLUÍDOS!")
        print("="*70)
        
    except requests.exceptions.ConnectionError:
        print("\n❌ ERRO: Não conectou à API")
        print("   Execute: python api.py")
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")

if __name__ == "__main__":
    run_all_tests()