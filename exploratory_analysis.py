import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class CheckoutAnalyzer:
    def __init__(self, csv1_path, csv2_path):
        """Inicializa o analisador com os arquivos CSV"""
        print("Carregando dados de checkout...")
        self.df1 = pd.read_csv(csv1_path)
        self.df2 = pd.read_csv(csv2_path)
        print(f"✓ Checkout 1: {len(self.df1)} registros")
        print(f"✓ Checkout 2: {len(self.df2)} registros")
        
    def explore_data(self):
        """Exploração inicial dos dados"""
        print("\n" + "="*60)
        print("ANÁLISE EXPLORATÓRIA - CHECKOUT 1")
        print("="*60)
        print("\nPrimeiras linhas:")
        print(self.df1.head(10))
        print("\nInformações do dataset:")
        print(self.df1.info())
        print("\nEstatísticas descritivas:")
        print(self.df1.describe())
        print("\nValores nulos:")
        print(self.df1.isnull().sum())
        
        print("\n" + "="*60)
        print("ANÁLISE EXPLORATÓRIA - CHECKOUT 2")
        print("="*60)
        print("\nPrimeiras linhas:")
        print(self.df2.head(10))
        
    def detect_anomalies_zscore(self, data, column, threshold=3):
        """Detecta anomalias usando Z-score"""
        mean = data[column].mean()
        std = data[column].std()
        
        if std == 0:
            return pd.DataFrame()
        
        z_scores = np.abs((data[column] - mean) / std)
        anomalies = data[z_scores > threshold].copy()
        anomalies['z_score'] = z_scores[z_scores > threshold]
        return anomalies
    
    def detect_anomalies_iqr(self, data, column):
        """Detecta anomalias usando IQR"""
        Q1 = data[column].quantile(0.25)
        Q3 = data[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        anomalies = data[(data[column] < lower_bound) | (data[column] > upper_bound)]
        return anomalies, lower_bound, upper_bound
    
    def analyze_checkout(self, df, name):
        """Análise específica de um checkout"""
        print("\n" + "="*60)
        print(f"ANÁLISE DE ANOMALIAS - {name}")
        print("="*60)
        
        numeric_cols = [col for col in df.columns if col != 'time']
        results = {}
        
        for col in numeric_cols:
            print(f"\n--- Análise da coluna: {col} ---")
            print(f"Média: {df[col].mean():.2f}")
            print(f"Mediana: {df[col].median():.2f}")
            print(f"Desvio Padrão: {df[col].std():.2f}")
            print(f"Min: {df[col].min():.2f} | Max: {df[col].max():.2f}")
            
            # Z-score
            anomalies_z = self.detect_anomalies_zscore(df, col)
            print(f"\nAnomalias Z-score (>3): {len(anomalies_z)}")
            if len(anomalies_z) > 0:
                print(anomalies_z[['time', col, 'z_score']].to_string())
            
            # IQR
            anomalies_iqr, lower, upper = self.detect_anomalies_iqr(df, col)
            print(f"\nAnomalias IQR: {len(anomalies_iqr)}")
            print(f"Limites: [{lower:.2f}, {upper:.2f}]")
            if len(anomalies_iqr) > 0:
                print(anomalies_iqr[['time', col]].to_string())
            
            results[col] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'anomalies_z': len(anomalies_z),
                'anomalies_iqr': len(anomalies_iqr)
            }
        
        return results
    
    def compare_today_vs_historical(self, df):
        """Compara vendas de hoje com histórico"""
        print("\n" + "="*60)
        print("COMPARAÇÃO: HOJE vs HISTÓRICO")
        print("="*60)
        
        if 'yesterday' in df.columns:
            df['var_yesterday'] = ((df['today'] - df['yesterday']) / df['yesterday'].replace(0, 1) * 100)
        
        if 'avg_last_week' in df.columns:
            df['var_last_week'] = ((df['today'] - df['avg_last_week']) / df['avg_last_week'].replace(0, 1) * 100)
        
        print("\nMAIORES VARIAÇÕES vs ONTEM:")
        if 'var_yesterday' in df.columns:
            top_var = df.nlargest(5, 'var_yesterday')[['time', 'today', 'yesterday', 'var_yesterday']]
            print(top_var.to_string())
        
        return df
    
    def create_visualizations(self, df1, df2):
        """Cria visualizações"""
        print("\n" + "="*60)
        print("GERANDO VISUALIZAÇÕES")
        print("="*60)
        
        os.makedirs('images', exist_ok=True)
        
        df1['hour'] = df1['time'].str.replace('h', '').astype(int)
        df2['hour'] = df2['time'].str.replace('h', '').astype(int)
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Análise de Checkouts', fontsize=16, fontweight='bold')
        
        # Gráfico 1: Checkout 1
        ax1 = axes[0, 0]
        ax1.plot(df1['hour'], df1['today'], label='Hoje', marker='o', linewidth=2)
        if 'yesterday' in df1.columns:
            ax1.plot(df1['hour'], df1['yesterday'], label='Ontem', marker='s', linewidth=2, alpha=0.7)
        if 'avg_last_week' in df1.columns:
            ax1.plot(df1['hour'], df1['avg_last_week'], label='Média Semana', marker='^', linewidth=2, alpha=0.7)
        ax1.set_title('Checkout 1: Comparação Temporal')
        ax1.set_xlabel('Hora')
        ax1.set_ylabel('Transações')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Gráfico 2: Checkout 2
        ax2 = axes[0, 1]
        ax2.plot(df2['hour'], df2['today'], label='Hoje', marker='o', linewidth=2, color='green')
        if 'yesterday' in df2.columns:
            ax2.plot(df2['hour'], df2['yesterday'], label='Ontem', marker='s', linewidth=2, alpha=0.7)
        ax2.set_title('Checkout 2: Comparação Temporal')
        ax2.set_xlabel('Hora')
        ax2.set_ylabel('Transações')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Gráfico 3: Boxplot
        ax3 = axes[1, 0]
        data_plot = [df1[col] for col in ['today', 'yesterday', 'avg_last_week'] if col in df1.columns]
        labels_plot = [col.replace('_', ' ').title() for col in ['today', 'yesterday', 'avg_last_week'] if col in df1.columns]
        bp = ax3.boxplot(data_plot, labels=labels_plot, patch_artist=True)
        for patch in bp['boxes']:
            patch.set_facecolor('lightblue')
        ax3.set_title('Checkout 1: Distribuição')
        ax3.set_ylabel('Transações')
        ax3.grid(True, alpha=0.3, axis='y')
        
        # Gráfico 4: Variação %
        ax4 = axes[1, 1]
        if 'yesterday' in df1.columns:
            var_pct = ((df1['today'] - df1['yesterday']) / df1['yesterday'].replace(0, 1) * 100)
            colors = ['red' if x < -20 else 'orange' if x < 0 else 'lightgreen' if x < 20 else 'green' for x in var_pct]
            ax4.bar(df1['hour'], var_pct, color=colors, alpha=0.7)
            ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
            ax4.set_title('Variação % (Hoje vs Ontem)')
            ax4.set_xlabel('Hora')
            ax4.set_ylabel('Variação %')
            ax4.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig('images/checkout_analysis.png', dpi=300, bbox_inches='tight')
        print("✓ Gráfico salvo: images/checkout_analysis.png")
        plt.close()
        
    def generate_report(self):
        """Gera relatório completo"""
        print("\n" + "="*60)
        print("GERANDO RELATÓRIO COMPLETO")
        print("="*60)
        
        self.explore_data()
        results1 = self.analyze_checkout(self.df1, "CHECKOUT 1")
        results2 = self.analyze_checkout(self.df2, "CHECKOUT 2")
        df1_analyzed = self.compare_today_vs_historical(self.df1)
        df2_analyzed = self.compare_today_vs_historical(self.df2)
        self.create_visualizations(df1_analyzed, df2_analyzed)
        
        print("\n✓ Análise completa finalizada!")
        return df1_analyzed, df2_analyzed, results1, results2

if __name__ == "__main__":
    analyzer = CheckoutAnalyzer('data/checkout_1.csv', 'data/checkout_2.csv')
    analyzer.generate_report()
    
    print("\n" + "="*60)
    print("CONCLUSÕES")
    print("="*60)
    print("""
    1. PADRÕES: Análise hora a hora comparando hoje vs histórico
    2. ANOMALIAS: Detectadas via Z-score e IQR
    3. RECOMENDAÇÕES: Monitorar variações > ±20%
    """)