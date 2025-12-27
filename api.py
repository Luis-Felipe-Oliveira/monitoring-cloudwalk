from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import sys
import os

# Adicionar diretório atual ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importar detector corrigido
try:
    from anomaly_detector import AnomalyDetector
except ImportError:
    print("ERRO: Não foi possível importar anomaly_detector.py")
    print("Certifique-se de que o arquivo está no mesmo diretório")
    sys.exit(1)

# Inicializar Flask
app = Flask(__name__)
CORS(app)

# Inicializar detector
print("\n" + "="*60)
print("INICIALIZANDO API FLASK")
print("="*60)

try:
    detector = AnomalyDetector(
        'data/transactions.csv',
        'data/transactions_auth_codes.csv'
    )
    print("✓ Detector inicializado com sucesso!")
except Exception as e:
    print(f"ERRO ao inicializar detector: {e}")
    detector = None

# Armazenamento em memória
alerts_history = []
transactions_buffer = []

@app.route('/')
def index():
    """Página inicial"""
    return jsonify({
        'message': 'CloudWalk Monitoring API',
        'version': '1.0.0',
        'status': 'online',
        'endpoints': {
            'POST /transaction': 'Recebe transação e retorna análise',
            'GET /alerts': 'Lista todos os alertas',
            'GET /alerts/active': 'Lista alertas críticos ativos',
            'GET /stats': 'Estatísticas do sistema',
            'GET /dashboard': 'Dados para dashboard',
            'GET /health': 'Health check'
        }
    })

@app.route('/transaction', methods=['POST'])
def receive_transaction():
    """
    Endpoint principal: recebe transação e retorna análise
    
    Aceita dois formatos:
    1. Transação individual: {"status": "approved", "amount": 100}
    2. Transação agregada: {"status": "approved", "count": 120}
    """
    if detector is None:
        return jsonify({'error': 'Detector não inicializado'}), 500
    
    try:
        if not request.is_json:
            return jsonify({'error': 'Content-Type deve ser application/json'}), 400
        
        transaction = request.get_json()
        
        # Validar campos obrigatórios
        if 'status' not in transaction:
            return jsonify({'error': 'Campo obrigatório: status'}), 400
        
        # Normalizar status
        transaction['status'] = transaction['status'].upper()
        
        # Se não tem count, assumir 1
        if 'count' not in transaction:
            transaction['count'] = 1
        
        # Adicionar timestamp
        if 'timestamp' not in transaction:
            transaction['timestamp'] = datetime.now().isoformat()
        
        # Adicionar ao buffer
        transactions_buffer.append(transaction)
        
        # Manter apenas últimas 100
        if len(transactions_buffer) > 100:
            transactions_buffer.pop(0)
        
        # Análise individual
        individual_analysis = detector.analyze_real_time(transaction)
        
        # Análise de janela (últimas 60)
        window_analysis = detector.analyze_transaction_window(transactions_buffer[-60:])
        
        # Salvar alerta se necessário
        if window_analysis['alert']:
            alert_record = {
                'id': len(alerts_history) + 1,
                'timestamp': datetime.now().isoformat(),
                'severity': window_analysis['severity'],
                'details': window_analysis['alerts'],
                'status_counts': window_analysis['status_counts']
            }
            alerts_history.append(alert_record)
        
        # Resposta
        response = {
            'success': True,
            'transaction_received': transaction,
            'individual_analysis': individual_analysis,
            'window_analysis': window_analysis,
            'recommendation': {
                'alert': window_analysis['alert'],
                'severity': window_analysis['severity'],
                'action': 'INVESTIGATE' if window_analysis['alert'] else 'MONITOR',
                'message': window_analysis['message']
            }
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500

@app.route('/alerts', methods=['GET'])
def get_alerts():
    """Retorna todos os alertas"""
    return jsonify({
        'total_alerts': len(alerts_history),
        'alerts': alerts_history[-50:]
    }), 200

@app.route('/alerts/active', methods=['GET'])
def get_active_alerts():
    """Retorna alertas críticos recentes"""
    now = datetime.now()
    active_alerts = []
    
    for alert in alerts_history:
        alert_time = datetime.fromisoformat(alert['timestamp'])
        time_diff = (now - alert_time).total_seconds() / 60
        
        if time_diff <= 10 and alert['severity'] == 'CRITICAL':
            active_alerts.append(alert)
    
    return jsonify({
        'active_critical_alerts': len(active_alerts),
        'alerts': active_alerts
    }), 200

@app.route('/stats', methods=['GET'])
def get_statistics():
    """Retorna estatísticas"""
    if detector is None:
        return jsonify({'error': 'Detector não inicializado'}), 500
    
    stats = detector.get_statistics()
    
    return jsonify({
        'detector_stats': stats,
        'api_stats': {
            'total_alerts_generated': len(alerts_history),
            'transactions_in_buffer': len(transactions_buffer),
            'uptime': 'Running'
        }
    }), 200

@app.route('/dashboard', methods=['GET'])
def get_dashboard_data():
    """Retorna dados para dashboard"""
    
    # Contar transações por status
    status_counts = {}
    total_count = 0
    
    for trans in transactions_buffer[-100:]:
        status = trans.get('status', 'UNKNOWN')
        count = trans.get('count', 1)
        status_counts[status] = status_counts.get(status, 0) + count
        total_count += count
    
    # Calcular taxa de erro
    errors = (status_counts.get('FAILED', 0) + 
              status_counts.get('DENIED', 0) + 
              status_counts.get('REJECTED', 0))
    error_rate = (errors / total_count * 100) if total_count > 0 else 0
    
    # Últimos alertas
    recent_alerts = alerts_history[-10:] if alerts_history else []
    
    return jsonify({
        'current_status': {
            'total_transactions': total_count,
            'status_distribution': status_counts,
            'error_rate_percent': round(error_rate, 2)
        },
        'recent_alerts': recent_alerts,
        'alerts_count': {
            'total': len(alerts_history),
            'critical': len([a for a in alerts_history if a['severity'] == 'CRITICAL']),
            'warning': len([a for a in alerts_history if a['severity'] == 'WARNING'])
        },
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/reset', methods=['POST'])
def reset_system():
    """Reseta o sistema"""
    global alerts_history, transactions_buffer
    alerts_history = []
    transactions_buffer = []
    
    return jsonify({
        'message': 'Sistema resetado',
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check"""
    return jsonify({
        'status': 'healthy',
        'detector_initialized': detector is not None,
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == '__main__':
    print("\n" + "="*60)
    print("CLOUDWALK MONITORING API")
    print("="*60)
    print("\n🚀 Iniciando servidor Flask...")
    print("\n📡 Endpoints disponíveis:")
    print("   POST   http://localhost:5000/transaction")
    print("   GET    http://localhost:5000/alerts")
    print("   GET    http://localhost:5000/alerts/active")
    print("   GET    http://localhost:5000/stats")
    print("   GET    http://localhost:5000/dashboard")
    print("   GET    http://localhost:5000/health")
    print("\n💡 Para testar:")
    print("   python test_api.py")
    print("\n" + "="*60 + "\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)