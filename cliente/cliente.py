"""
TRABALHO 3 - REPLICAÇÃO DE DADOS, ELEIÇÃO E TOLERÂNCIA A FALHAS (gRPC, MODELO PULL)
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Cliente: grava no líder (com redirecionamento) e lê os dados comitados de qualquer nó do cluster.
"""

import os
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stdin.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# Permite importar os utilitários e os stubs gerados da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)

import grpc

import replicacao_pb2 as pb
import replicacao_pb2_grpc as pb_grpc
from config import NOS, OPCOES_CANAL, TIMEOUT_COMMIT

TIMEOUT_CONSULTA = 1.0
NOME_PAPEL = {'seguidor': 'SEGUIDOR', 'candidato': 'CANDIDATO', 'lider': 'LÍDER'}

stubs = {nid: pb_grpc.ArmazenamentoStub(grpc.insecure_channel(end, options=OPCOES_CANAL)) for nid, end in NOS.items()}
lider_conhecido = None

AJUDA = f"""
Comandos disponíveis:
  gravar <chave> <valor>   Grava no líder (redireciona se cair em um seguidor)
  ler <id> [chave]         Lê os dados COMITADOS do nó <id> (todos, se não houver chave)
  status                   Papel, termo, commit e líder de cada nó
  log <id>                 Log completo do nó <id>  (* = comitado)
  crash <chave> <valor>    Grava no líder e derruba o líder antes de replicar (cenário 4B)
  segurar | liberar        Segura/libera o avanço do commit no líder (cenário 5)
  help / ?                 Exibe esta ajuda
  quit / exit              Encerra o cliente
  (ids válidos: {', '.join(map(str, NOS))})
"""

# --- Funções utilitárias ---

def descrever(estado):
    lider = f'Nó {estado.lider_id}' if estado.lider_id else '?'
    return (f'{NOME_PAPEL.get(estado.papel, estado.papel):9s} termo={estado.termo:<3d} '
            f'commit={estado.commit_index:<3d} log={estado.ultimo_indice:<3d} líder={lider}')

def encontrar_lider():
    """Pergunta o estado a todos os nós e devolve o id do líder de maior termo (ou None)."""
    melhor = None
    for nid, stub in stubs.items():
        try:
            estado = stub.ObterEstado(pb.Vazio(), timeout=TIMEOUT_CONSULTA)
        except grpc.RpcError:
            continue
        if estado.papel == 'lider' and (melhor is None or estado.termo > melhor[1]):
            melhor = (nid, estado.termo)
    return melhor[0] if melhor else None

def proximo_nao_tentado(tentados):
    for nid in NOS:
        if nid not in tentados:
            return nid
    return None

# --- Gravação com redirecionamento ---

def cmd_gravar(chave, valor):
    global lider_conhecido
    pedido = pb.GravarRequest(chave=chave, valor=valor)
    alvo = lider_conhecido or next(iter(NOS))
    tentados = set()

    for _ in range(2 * len(NOS)):
        tentados.add(alvo)
        print(f'  Enviando gravação ao Nó {alvo}...')
        try:
            resposta = stubs[alvo].Gravar(pedido, timeout=TIMEOUT_COMMIT + 2)
        except grpc.RpcError as erro:
            if erro.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                print(f'  Sem resposta do Nó {alvo} no prazo. A escrita pode ou não ter sido comitada.')
                return
            print(f'  Nó {alvo} inacessível.')
            if lider_conhecido == alvo:
                lider_conhecido = None
            alvo = proximo_nao_tentado(tentados)
            if alvo is None:
                break
            continue

        if resposta.status == pb.NAO_LIDER:
            if resposta.lider_id and resposta.lider_id != alvo:
                print(f'  {resposta.mensagem}: redirecionando para o líder, Nó {resposta.lider_id}')
                alvo = resposta.lider_id
            else:
                print(f'  {resposta.mensagem} e não conhece o líder atual.')
                alvo = proximo_nao_tentado(tentados)
                if alvo is None:
                    break
            continue

        lider_conhecido = alvo
        if resposta.status == pb.OK:
            print(f'  OK: {chave}={valor} comitado no índice {resposta.indice} (termo {resposta.termo}) pelo líder Nó {alvo}')
        elif resposta.status == pb.SEM_QUORUM:
            print(f'  RECUSADA pelo líder Nó {alvo}: {resposta.mensagem}')
        elif resposta.status == pb.PENDENTE:
            print(f'  PENDENTE no líder Nó {alvo}: {resposta.mensagem}')
        return

    print('  Nenhum líder encontrado. Aguarde a eleição terminar e tente de novo.')

def cmd_crash(chave, valor):
    """Cenário 4B: o líder grava a entrada no log local e encerra o processo antes de replicar."""
    global lider_conhecido
    lider = encontrar_lider()
    if lider is None:
        print('  Nenhum líder encontrado.')
        return
    print(f'  Enviando {chave}={valor} ao líder Nó {lider} com queda simulada...')
    try:
        stubs[lider].Gravar(pb.GravarRequest(chave=chave, valor=valor, simular_queda=True), timeout=TIMEOUT_COMMIT)
        print('  O líder respondeu (inesperado): a queda não foi simulada.')
    except grpc.RpcError:
        print(f'  Nó {lider} caiu após gravar no log local. A entrada NÃO foi replicada nem comitada.')
        print(f'  Religue-o depois com: python no_cluster\\no_cluster.py {lider}')
    lider_conhecido = None

# --- Consulta e estado ---

def cmd_ler(nid, chave):
    try:
        resposta = stubs[nid].Consultar(pb.ConsultarRequest(chave=chave or ''), timeout=TIMEOUT_CONSULTA)
    except grpc.RpcError:
        print(f'  Nó {nid} inacessível.')
        return
    print(f'  Nó {nid} [{descrever(resposta.estado)}]')
    if not resposta.registros:
        print(f'    chave "{chave}" não encontrada nos dados comitados' if chave else '    (nenhum dado comitado)')
    for r in resposta.registros:
        print(f'    {r.chave} = {r.valor}   (índice {r.indice})')

def cmd_status():
    for nid, stub in stubs.items():
        try:
            estado = stub.ObterEstado(pb.Vazio(), timeout=TIMEOUT_CONSULTA)
        except grpc.RpcError:
            print(f'  Nó {nid} [INACESSÍVEL]')
            continue
        extra = ''
        if estado.papel == 'lider':
            extra = f'  réplicas ativas={list(estado.replicas_ativas)}'
            if estado.commit_segurado:
                extra += '  (commit SEGURADO)'
        print(f'  Nó {nid} [{descrever(estado)}]{extra}')

def cmd_log(nid):
    try:
        estado = stubs[nid].ObterEstado(pb.Vazio(), timeout=TIMEOUT_CONSULTA)
    except grpc.RpcError:
        print(f'  Nó {nid} inacessível.')
        return
    print(f'  Log do Nó {nid} [{descrever(estado)}]')
    if not estado.log:
        print('    (vazio)')
    for e in estado.log:
        marca = '*' if e.indice <= estado.commit_index else ' '
        print(f'    [{e.indice:3d}]{marca} termo={e.termo:<3d} {e.chave} = {e.valor}')
    print('    * = comitado (visível na leitura)')

def cmd_depurar(acao):
    lider = encontrar_lider()
    if lider is None:
        print('  Nenhum líder encontrado.')
        return
    try:
        estado = stubs[lider].Depurar(pb.DepurarRequest(acao=acao), timeout=TIMEOUT_CONSULTA)
    except grpc.RpcError:
        print(f'  Nó {lider} inacessível.')
        return
    situacao = 'SEGURADO' if estado.commit_segurado else 'LIBERADO'
    print(f'  Commit {situacao} no líder Nó {lider} [{descrever(estado)}]')

# --- Laço principal ---

def ler_id(texto):
    if not texto.isdigit() or int(texto) not in NOS:
        return None
    return int(texto)

def executar(linha):
    """Executa um comando. Devolve False quando o cliente deve encerrar."""
    partes = linha.split()
    if not partes:
        return True
    cmd, args = partes[0].lower(), partes[1:]

    if cmd in ('quit', 'exit'):
        return False
    elif cmd in ('help', '?'):
        print(AJUDA)
    elif cmd in ('gravar', 'crash'):
        if len(args) < 2:
            print(f'  Uso: {cmd} <chave> <valor>')
        else:
            (cmd_gravar if cmd == 'gravar' else cmd_crash)(args[0], ' '.join(args[1:]))
    elif cmd == 'ler':
        nid = ler_id(args[0]) if args else None
        if nid is None:
            print(f'  Uso: ler <id> [chave]   (ids válidos: {list(NOS)})')
        else:
            cmd_ler(nid, args[1] if len(args) > 1 else None)
    elif cmd == 'log':
        nid = ler_id(args[0]) if args else None
        if nid is None:
            print(f'  Uso: log <id>   (ids válidos: {list(NOS)})')
        else:
            cmd_log(nid)
    elif cmd == 'status':
        cmd_status()
    elif cmd == 'segurar':
        cmd_depurar('segurar_commit')
    elif cmd == 'liberar':
        cmd_depurar('liberar_commit')
    else:
        print(f'  Comando desconhecido: "{cmd}". Digite "help" para ver os comandos.')
    return True

def main():
    print('=== Cliente: replicação de dados com gRPC (modelo Pull) ===')
    print(AJUDA)
    while True:
        try:
            linha = input('> ').strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not executar(linha):
            break
    print('Cliente encerrado.')

if __name__ == '__main__':
    main()
