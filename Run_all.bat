@echo off
chcp 65001 >nul
cls
echo ============================================================
echo   CLOUDWALK MONITORING SYSTEM - SETUP AUTOMATICO
echo ============================================================
echo.
echo Iniciando configuracao automatica...
echo.

REM Verificar Python
echo [1/7] Verificando Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python nao encontrado!
    echo.
    echo Por favor, instale Python 3.8+ de: https://www.python.org/downloads/
    pause
    exit /b 1
)
echo [OK] Python encontrado!
echo.

REM Verificar dados
echo [2/7] Verificando arquivos CSV...
if exist "data\checkout_1.csv" (
    echo [OK] checkout_1.csv encontrado
) else (
    echo [ERRO] data\checkout_1.csv nao encontrado!
    echo Por favor, coloque os arquivos CSV na pasta data\
    pause
    exit /b 1
)
if exist "data\transactions.csv" (
    echo [OK] transactions.csv encontrado
) else (
    echo [ERRO] data\transactions.csv nao encontrado!
    pause
    exit /b 1
)
echo.

REM Criar ambiente virtual
echo [3/7] Criando ambiente virtual...
if not exist "venv" (
    python -m venv venv
    echo [OK] Ambiente virtual criado!
) else (
    echo [OK] Ambiente virtual ja existe!
)
echo.

REM Ativar ambiente virtual
echo [4/7] Ativando ambiente virtual...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERRO] Falha ao ativar ambiente virtual!
    pause
    exit /b 1
)
echo [OK] Ambiente virtual ativado!
echo.

REM Instalar dependencias
echo [5/7] Instalando dependencias...
echo Isso pode levar alguns minutos...
python -m pip install --upgrade pip >nul 2>&1
python -m pip install -r requirements.txt >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Falha ao instalar dependencias!
    echo Tentando instalar manualmente...
    pip install Flask flask-cors pandas numpy matplotlib seaborn
    if %errorlevel% neq 0 (
        echo [ERRO] Instalacao falhou!
        pause
        exit /b 1
    )
)
echo [OK] Dependencias instaladas!
echo.

REM Verificar instalacao
echo [6/7] Verificando instalacao...
python -c "import flask; print('[OK] Flask:', flask.__version__)" 2>nul
if %errorlevel% neq 0 (
    echo [ERRO] Flask nao foi instalado corretamente!
    pause
    exit /b 1
)
python -c "import pandas; print('[OK] Pandas:', pandas.__version__)" 2>nul
echo.

REM Executar analises
echo [7/7] Executando analises iniciais...
echo.
echo Executando analise exploratoria...
python exploratory_analysis.py
if %errorlevel% neq 0 (
    echo [AVISO] Erro na analise exploratoria (continuando...)
)
echo.
echo Executando analise SQL...
python sql_analysis.py
if %errorlevel% neq 0 (
    echo [AVISO] Erro na analise SQL (continuando...)
)
echo.

echo.
echo Executando detector de anomalias...
python anomaly_detector.py
if %errorlevel% neq 0 (
    echo [AVISO] Erro no detector de anomalias (continuando...)
) else (
    echo [OK] Detector de anomalias executado com sucesso!
)
echo.

REM Sucesso
echo ============================================================
echo   SETUP COMPLETO!
echo ============================================================
echo.
echo [OK] Sistema configurado e pronto para uso!
echo.
echo Graficos gerados em: images\
echo Queries SQL em: queries\
echo.
echo ============================================================
echo   INICIANDO API...
echo ============================================================
echo.
echo A API sera iniciada em http://localhost:5000
echo.
echo Para usar:
echo   1. API esta rodando (este terminal)
echo   2. Abra dashboard.html no navegador
echo   3. Para testar: abra novo CMD e execute 'python test_api.py'
echo.
echo Pressione Ctrl+C para parar a API
echo ============================================================
echo.

python api.py

REM Se API foi parada
echo.
echo API encerrada.
pause