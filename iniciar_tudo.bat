@echo off
REM Abre os 4 nos do cluster e o cliente, cada um em uma janela separada
REM Para comecar do zero (termo 0, log vazio), apague a pasta "dados" antes
cd /d "%~dp0"

python -c "import grpc, grpc_tools" 2>nul || python -m pip install -r requirements.txt
python -m grpc_tools.protoc -I proto --python_out=. --pyi_out=. --grpc_python_out=. proto\replicacao.proto

start "No 1" cmd /k python no_cluster\no_cluster.py 1
start "No 2" cmd /k python no_cluster\no_cluster.py 2
start "No 3" cmd /k python no_cluster\no_cluster.py 3
start "No 4" cmd /k python no_cluster\no_cluster.py 4

timeout /t 2 /nobreak >nul
start "Cliente" cmd /k python cliente\cliente.py
