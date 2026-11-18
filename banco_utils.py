"""
TRABALHO 3 - REPLICAÇÃO DE DADOS, ELEIÇÃO E TOLERÂNCIA A FALHAS (gRPC, MODELO PULL)
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Função compartilhada de acesso ao SQLite (reaproveitada do Projeto 2). Cada nó tem o seu próprio banco.
"""

import os
import sqlite3
from contextlib import contextmanager

@contextmanager
def abrir_banco(caminho):
    """Abre uma conexão, confirma as alterações ao final e fecha."""
    os.makedirs(os.path.dirname(caminho), exist_ok=True)

    # Uma conexão por operação: as threads não podem compartilhar a mesma
    banco = sqlite3.connect(caminho, timeout=10)
    banco.row_factory = sqlite3.Row
    try:
        yield banco
        banco.commit()
    except Exception:
        banco.rollback()
        raise
    finally:
        banco.close()
