"""
TRABALHO 3 - REPLICAÇÃO DE DADOS, ELEIÇÃO E TOLERÂNCIA A FALHAS (gRPC, MODELO PULL)
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Configurações compartilhadas pelos nós do cluster e pelo cliente.
"""

import os

# --- Endereços dos nós ---
# Usa 127.0.0.1: no Windows, "localhost" tenta antes o IPv6 e perde tempo em cada conexão
NOS = {
    1: '127.0.0.1:50051',
    2: '127.0.0.1:50052',
    3: '127.0.0.1:50053',
    4: '127.0.0.1:50054',
}

QUORUM = len(NOS) // 2 + 1      # 3 de 4 (maioria absoluta)

# Banco SQLite de cada nó: <PASTA_DADOS>/no<id>.db (apague a pasta para começar do zero)
PASTA_DADOS = os.environ.get('PASTA_DADOS', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dados'))

# Opções dos canais gRPC: tenta reconectar rápido a um nó que voltou
# (o padrão do gRPC chega a esperar até 120 s entre tentativas)
OPCOES_CANAL = [
    ('grpc.initial_reconnect_backoff_ms', 100),
    ('grpc.min_reconnect_backoff_ms', 100),
    ('grpc.max_reconnect_backoff_ms', 500),
]

# --- Temporizadores (segundos) ---
FETCH_INTERVALO     = 0.1       # Réplica pede dados ao líder a cada 100 ms
HEARTBEAT_TIMEOUT   = 0.5       # Líder marca a réplica como inativa (cerca de 5 fetches perdidos)
ELEICAO_TIMEOUT_MIN = 1.5       # Temporizador aleatório de cada réplica
ELEICAO_TIMEOUT_MAX = 3.0
TIMEOUT_RPC         = 0.3       # Deadline de cada chamada gRPC entre os nós
TIMEOUT_COMMIT      = 5.0       # Quanto o líder espera o commit antes de responder PENDENTE
STATUS_INTERVALO    = 5.0       # Linha periódica de status na janela do nó

MAX_ENTRADAS_FETCH  = 50        # Lote máximo de entradas por FetchResponse

# --- Papéis ---
SEGUIDOR  = 'seguidor'
CANDIDATO = 'candidato'
LIDER     = 'lider'
