import os

print("="*60)
print("DOWNLOAD DE DADOS - CloudWalk Monitoring")
print("="*60)

# Criar pasta data se não existir
os.makedirs('data', exist_ok=True)

print("\n⚠️  IMPORTANTE:")
print("Este script é um placeholder para download dos dados.")
print("\nPor favor, coloque manualmente os seguintes arquivos na pasta 'data/':")
print("\n  1. checkout_1.csv")
print("  2. checkout_2.csv")
print("  3. transactions.csv")
print("  4. transactions_auth_codes.csv")

print("\n📂 Estrutura esperada:")
print("""
monitoring-cloudwalk/
├── data/
│   ├── checkout_1.csv
│   ├── checkout_2.csv
│   ├── transactions.csv
│   └── transactions_auth_codes.csv
""")

# Verificar se arquivos existem
files_to_check = [
    'data/checkout_1.csv',
    'data/checkout_2.csv',
    'data/transactions.csv',
    'data/transactions_auth_codes.csv'
]

print("\n🔍 Verificando arquivos...")
all_present = True

for file in files_to_check:
    if os.path.exists(file):
        size = os.path.getsize(file)
        print(f"  ✅ {file} ({size} bytes)")
    else:
        print(f"  ❌ {file} - NÃO ENCONTRADO")
        all_present = False

if all_present:
    print("\n✅ Todos os arquivos estão presentes!")
    print("\nPróximo passo:")
    print("  python exploratory_analysis.py")
else:
    print("\n⚠️  Alguns arquivos estão faltando.")
    print("Por favor, adicione-os na pasta 'data/' antes de continuar.")