"""
TRABALHO 3 - REPLICAÇÃO DE DADOS, ELEIÇÃO E TOLERÂNCIA A FALHAS (gRPC, MODELO PULL)
BSI
Disciplina: Sistemas Distribuídos
Professora: Ana Cristina Barreiras Kochem Vendramin

Aluno: Vitor Chiuco Zeni

Nó do cluster: o mesmo código assume os papéis de seguidor, candidato e líder.
Uso: python no_cluster/no_cluster.py <id>   (id: 1 a 4)
"""

import os
import random
import sys
import threading
import time
from concurrent import futures

# Evita UnicodeEncodeError em consoles que não suportam UTF-8 (ex.: cmd.exe)
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# Permite importar os utilitários e os stubs gerados da raiz do projeto
raiz_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, raiz_projeto)

import grpc

import replicacao_pb2 as pb
import replicacao_pb2_grpc as pb_grpc
from banco_utils import abrir_banco
from config import (
    NOS, QUORUM, PASTA_DADOS, OPCOES_CANAL,
    FETCH_INTERVALO, HEARTBEAT_TIMEOUT, ELEICAO_TIMEOUT_MIN, ELEICAO_TIMEOUT_MAX,
    TIMEOUT_RPC, TIMEOUT_COMMIT, STATUS_INTERVALO, MAX_ENTRADAS_FETCH,
    SEGUIDOR, CANDIDATO, LIDER,
)

NOME_PAPEL = {SEGUIDOR: 'SEGUIDOR', CANDIDATO: 'CANDIDATO', LIDER: 'LÍDER'}

# --- Persistência ---

class Persistencia:
    """Termo, voto, log e banco final de um nó, gravados em SQLite."""

    def __init__(self, caminho):
        self.caminho = caminho
        self.criar_tabelas()

    def criar_tabelas(self):
        with abrir_banco(self.caminho) as banco:
            banco.executescript('''
                CREATE TABLE IF NOT EXISTS estado (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS log (
                    indice INTEGER PRIMARY KEY,
                    termo  INTEGER NOT NULL,
                    chave  TEXT NOT NULL,
                    valor  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS banco (
                    chave  TEXT PRIMARY KEY,
                    valor  TEXT NOT NULL,
                    indice INTEGER NOT NULL
                );
            ''')

    def carregar(self):
        """Devolve (termo, voto, log, ultimo_aplicado) gravados no disco."""
        with abrir_banco(self.caminho) as banco:
            estado = {linha['chave']: linha['valor'] for linha in banco.execute('SELECT chave, valor FROM estado')}
            log = [
                pb.Entrada(indice=linha['indice'], termo=linha['termo'], chave=linha['chave'], valor=linha['valor'])
                for linha in banco.execute('SELECT indice, termo, chave, valor FROM log ORDER BY indice')
            ]
        return (
            int(estado.get('termo_atual', 0)),
            int(estado.get('voto', 0)),
            log,
            int(estado.get('ultimo_aplicado', 0)),
        )

    def salvar_termo_voto(self, termo, voto):
        with abrir_banco(self.caminho) as banco:
            banco.executemany(
                'INSERT OR REPLACE INTO estado (chave, valor) VALUES (?, ?)',
                [('termo_atual', str(termo)), ('voto', str(voto))],
            )

    def anexar_entradas(self, entradas):
        with abrir_banco(self.caminho) as banco:
            banco.executemany(
                'INSERT OR REPLACE INTO log (indice, termo, chave, valor) VALUES (?, ?, ?, ?)',
                [(e.indice, e.termo, e.chave, e.valor) for e in entradas],
            )

    def truncar_log(self, depois_de):
        with abrir_banco(self.caminho) as banco:
            banco.execute('DELETE FROM log WHERE indice > ?', (depois_de,))

    def aplicar(self, entradas, novo_commit):
        """Grava as entradas comitadas no banco final e o ultimo_aplicado na mesma transação."""
        with abrir_banco(self.caminho) as banco:
            banco.executemany(
                '''INSERT INTO banco (chave, valor, indice) VALUES (?, ?, ?)
                   ON CONFLICT(chave) DO UPDATE SET valor = excluded.valor, indice = excluded.indice''',
                [(e.chave, e.valor, e.indice) for e in entradas],
            )
            banco.execute(
                'INSERT OR REPLACE INTO estado (chave, valor) VALUES (?, ?)',
                ('ultimo_aplicado', str(novo_commit)),
            )

    def consultar(self, chave=None):
        with abrir_banco(self.caminho) as banco:
            if chave:
                linhas = banco.execute('SELECT chave, valor, indice FROM banco WHERE chave = ?', (chave,))
            else:
                linhas = banco.execute('SELECT chave, valor, indice FROM banco ORDER BY chave')
            return [pb.Registro(chave=l['chave'], valor=l['valor'], indice=l['indice']) for l in linhas]

# --- Nó ---

class NoCluster:
    def __init__(self, no_id):
        self.no_id = no_id
        self.peers = [nid for nid in NOS if nid != no_id]
        self.disco = Persistencia(os.path.join(PASTA_DADOS, f'no{no_id}.db'))

        # Estado persistente (recarregado do disco)
        self.termo, self.voto, self.log, ultimo_aplicado = self.disco.carregar()
        self.commit_index = ultimo_aplicado

        # Estado volátil: todo nó começa como seguidor e sem líder conhecido
        self.papel = SEGUIDOR
        self.lider_atual = 0
        self.votos = 0
        self.rodizio = 0

        # Estado exclusivo do líder
        self.match_index = {}          # Último índice confirmado por réplica (via Fetch)
        self.ultimo_fetch = {}         # Instante do último Fetch de cada réplica
        self.replicas_ativas = set()   # Só para avisar quando uma réplica cai ou volta
        self.commit_segurado = False   # Cenário 5 (Depurar)

        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)   # Gravar espera o commit avançar aqui
        self.temporizador = None
        self.geracao_temporizador = 0
        self.ultimo_titulo = ''

        self.stubs = {
            nid: pb_grpc.ClusterStub(grpc.insecure_channel(NOS[nid], options=OPCOES_CANAL))
            for nid in self.peers
        }

        self.mostrar(f'Estado carregado do disco: termo={self.termo} voto={self.voto or "-"} '
                     f'log={len(self.log)} entradas commit={self.commit_index}')
        with self.lock:
            self._reiniciar_temporizador()
        self.mostrar(f'Iniciado como SEGUIDOR | termo={self.termo}')

    # --- Utilitários (os métodos com "_" são chamados com o lock adquirido) ---

    def mostrar(self, mensagem):
        agora = time.strftime('%H:%M:%S') + f'.{int(time.time() * 1000) % 1000:03d}'
        print(f'{agora} [Nó {self.no_id}] {mensagem}')

    def _ultimo_indice(self):
        return len(self.log)

    def _ultimo_termo(self):
        return self.log[-1].termo if self.log else 0

    def _termo_em(self, indice):
        return self.log[indice - 1].termo if 1 <= indice <= len(self.log) else 0

    def _reiniciar_temporizador(self):
        """Temporizador de eleição aleatório. Cada reinício invalida o anterior pela geração."""
        if self.temporizador:
            self.temporizador.cancel()
        self.geracao_temporizador += 1
        tempo = random.uniform(ELEICAO_TIMEOUT_MIN, ELEICAO_TIMEOUT_MAX)
        self.temporizador = threading.Timer(tempo, self._iniciar_eleicao, args=(self.geracao_temporizador,))
        self.temporizador.daemon = True
        self.temporizador.start()

    def _cancelar_temporizador(self):
        if self.temporizador:
            self.temporizador.cancel()
            self.temporizador = None
        self.geracao_temporizador += 1

    def _tornar_seguidor(self, termo, lider=0):
        """Atualização obrigatória de termo: vira seguidor e, se o termo subiu, zera o voto."""
        if termo > self.termo:
            self.termo = termo
            self.voto = 0
            self.disco.salvar_termo_voto(self.termo, self.voto)
            self.lider_atual = 0
        if self.papel != SEGUIDOR:
            self.mostrar(f'-> SEGUIDOR | termo={self.termo}')
        self.papel = SEGUIDOR
        if lider:
            self._definir_lider(lider)
        self._reiniciar_temporizador()
        self.cond.notify_all()   # Acorda um Gravar que esperava commit como líder

    def _definir_lider(self, lider):
        if lider != self.lider_atual:
            self.mostrar(f'Líder atual: Nó {lider} (termo={self.termo})')
        self.lider_atual = lider

    def _tornar_lider(self):
        self.papel = LIDER
        self.lider_atual = self.no_id
        self._cancelar_temporizador()
        # Começa com todas as réplicas inativas: só aceita escrita depois que elas fizerem Fetch
        self.match_index = {nid: 0 for nid in self.peers}
        self.ultimo_fetch = {nid: 0.0 for nid in self.peers}
        self.replicas_ativas = set()
        self.mostrar(f'*** ELEITO LÍDER *** | termo={self.termo} | votos={self.votos}')

    def _ativas(self):
        agora = time.monotonic()
        return sorted(nid for nid in self.peers if agora - self.ultimo_fetch.get(nid, 0.0) < HEARTBEAT_TIMEOUT)

    def _avancar_commit(self):
        """Avança o commit para o maior índice presente em pelo menos QUORUM nós."""
        if self.commit_segurado:
            return
        indices = sorted([self._ultimo_indice()] + [self.match_index.get(nid, 0) for nid in self.peers], reverse=True)
        n = indices[QUORUM - 1]
        # Regra de segurança do Raft: só comita por contagem entradas do termo atual
        if n > self.commit_index and self._termo_em(n) == self.termo:
            self.mostrar(f'Quórum confirmou até o índice {n} (match_index={self.match_index})')
            self._aplicar_commit(n)

    def _aplicar_commit(self, novo_commit):
        """Efetiva as entradas no banco final, tornando-as visíveis para leitura."""
        entradas = self.log[self.commit_index:novo_commit]
        self.disco.aplicar(entradas, novo_commit)
        self.commit_index = novo_commit
        for e in entradas:
            self.mostrar(f'APLICADO índice={e.indice} termo={e.termo} {e.chave}={e.valor} | commit={self.commit_index}')
        self.cond.notify_all()

    def _estado(self):
        return pb.EstadoNo(
            no_id=self.no_id,
            papel=self.papel,
            termo=self.termo,
            voto=self.voto,
            lider_id=self.lider_atual,
            commit_index=self.commit_index,
            ultimo_indice=self._ultimo_indice(),
            log=self.log,
            replicas_ativas=self._ativas() if self.papel == LIDER else [],
            commit_segurado=self.commit_segurado,
        )

    # --- Eleição (lado do candidato) ---

    def _iniciar_eleicao(self, geracao):
        with self.lock:
            if geracao != self.geracao_temporizador or self.papel == LIDER:
                return
            self.papel = CANDIDATO
            self.termo += 1
            self.voto = self.no_id
            self.disco.salvar_termo_voto(self.termo, self.voto)   # Antes de pedir votos
            self.votos = 1
            self.lider_atual = 0
            pedido = pb.VoteRequest(
                termo=self.termo,
                candidato_id=self.no_id,
                ultimo_indice=self._ultimo_indice(),
                ultimo_termo=self._ultimo_termo(),
            )
            self.mostrar(f'Temporizador expirou -> CANDIDATO | termo={self.termo} | pedindo votos')
            self._reiniciar_temporizador()   # Se a votação empatar, outra eleição começa

        for nid in self.peers:
            threading.Thread(target=self._pedir_voto, args=(nid, pedido), daemon=True).start()

    def _pedir_voto(self, nid, pedido):
        try:
            resposta = self.stubs[nid].RequestVote(pedido, timeout=TIMEOUT_RPC)
        except grpc.RpcError:
            return

        with self.lock:
            if resposta.termo > self.termo:
                self._tornar_seguidor(resposta.termo)
                return
            if self.papel != CANDIDATO or self.termo != pedido.termo:
                return
            if resposta.voto_concedido:
                self.votos += 1
                self.mostrar(f'Voto recebido do Nó {nid} | total={self.votos}/{QUORUM}')
                if self.votos >= QUORUM:
                    self._tornar_lider()

    # --- RequestVote (lado de quem vota) ---

    def receber_pedido_voto(self, pedido):
        with self.lock:
            if pedido.termo > self.termo:
                self._tornar_seguidor(pedido.termo)

            # Validação de log: o do candidato precisa ser tão ou mais atualizado que o meu
            meu_termo, meu_indice = self._ultimo_termo(), self._ultimo_indice()
            log_ok = (pedido.ultimo_termo > meu_termo or
                      (pedido.ultimo_termo == meu_termo and pedido.ultimo_indice >= meu_indice))
            voto_livre = self.voto in (0, pedido.candidato_id)   # Voto único por termo

            concedido = pedido.termo == self.termo and voto_livre and log_ok
            if concedido:
                if self.voto != pedido.candidato_id:
                    self.voto = pedido.candidato_id
                    self.disco.salvar_termo_voto(self.termo, self.voto)   # Antes de responder
                self._reiniciar_temporizador()
                self.lider_atual = pedido.candidato_id   # Dica: o primeiro Fetch tenta nele
                motivo = ''
            elif pedido.termo < self.termo:
                motivo = f' (termo antigo, o meu é {self.termo})'
            elif not voto_livre:
                motivo = f' (já votei no Nó {self.voto} neste termo)'
            else:
                motivo = f' (log desatualizado: candidato {pedido.ultimo_termo}/{pedido.ultimo_indice}, meu {meu_termo}/{meu_indice})'

            self.mostrar(f'Voto {"CONCEDIDO" if concedido else "NEGADO"} ao Nó {pedido.candidato_id} '
                         f'| termo={pedido.termo}{motivo}')
            return pb.VoteResponse(termo=self.termo, voto_concedido=concedido)

    # --- Fetch (lado do líder) ---

    def atender_fetch(self, pedido):
        with self.lock:
            if pedido.termo > self.termo:
                self._tornar_seguidor(pedido.termo)
                return pb.FetchResponse(termo=self.termo, status=pb.NAO_LIDER, lider_id=0)
            if self.papel != LIDER:
                return pb.FetchResponse(termo=self.termo, status=pb.NAO_LIDER, lider_id=self.lider_atual)

            replica = pedido.replica_id
            self.ultimo_fetch[replica] = time.monotonic()   # Heartbeat da réplica
            ultimo = self._ultimo_indice()

            # Verificação de consistência: a última entrada da réplica existe no meu log com o mesmo termo?
            if pedido.ultimo_indice > ultimo or self._termo_em(pedido.ultimo_indice) != pedido.ultimo_termo:
                # A entrada em ultimo_indice conflita (ou não existe aqui): o ponto consistente é anterior.
                # Pula as minhas entradas com termo maior que o da réplica, que certamente divergem.
                x = min(pedido.ultimo_indice - 1, ultimo)
                while x > 0 and self._termo_em(x) > pedido.ultimo_termo:
                    x -= 1
                self.mostrar(f'Fetch do Nó {replica} DIVERGENTE (réplica em {pedido.ultimo_indice}/termo '
                             f'{pedido.ultimo_termo}) -> truncar após o índice {x}')
                return pb.FetchResponse(
                    termo=self.termo, status=pb.DIVERGENTE, lider_id=self.no_id,
                    commit_index=self.commit_index, indice_consistente=x,
                )

            # Consistente: a réplica tem tudo até ultimo_indice
            self.match_index[replica] = pedido.ultimo_indice
            self._avancar_commit()

            entradas = self.log[pedido.ultimo_indice:pedido.ultimo_indice + MAX_ENTRADAS_FETCH]
            if entradas:
                self.mostrar(f'Fetch do Nó {replica}: enviando entradas {entradas[0].indice}..{entradas[-1].indice} '
                             f'| commit={self.commit_index}')
            return pb.FetchResponse(
                termo=self.termo, status=pb.OK, lider_id=self.no_id,
                entradas=entradas, commit_index=self.commit_index,
            )

    # --- Loop de Fetch (lado da réplica) ---

    def _escolher_alvo(self):
        """Líder conhecido ou, se desconhecido, o próximo nó do rodízio (descoberta do líder)."""
        if self.lider_atual and self.lider_atual != self.no_id:
            return self.lider_atual
        self.rodizio = (self.rodizio + 1) % len(self.peers)
        return self.peers[self.rodizio]

    def loop_fetch(self):
        while True:
            time.sleep(FETCH_INTERVALO)
            with self.lock:
                if self.papel == LIDER:
                    continue
                alvo = self._escolher_alvo()
                pedido = pb.FetchRequest(
                    termo=self.termo,
                    replica_id=self.no_id,
                    ultimo_indice=self._ultimo_indice(),
                    ultimo_termo=self._ultimo_termo(),
                )

            try:
                resposta = self.stubs[alvo].Fetch(pedido, timeout=TIMEOUT_RPC)
            except grpc.RpcError:
                with self.lock:
                    if self.lider_atual == alvo:
                        self.mostrar(f'Líder (Nó {alvo}) não responde ao Fetch')
                        self.lider_atual = 0
                continue

            with self.lock:
                self._tratar_resposta_fetch(alvo, pedido, resposta)

    def _tratar_resposta_fetch(self, alvo, pedido, resposta):
        if resposta.termo > self.termo:
            self._tornar_seguidor(resposta.termo)   # Atualização obrigatória de termo

        if resposta.status == pb.NAO_LIDER:
            if self.lider_atual == alvo:
                self.lider_atual = 0
            if resposta.lider_id and resposta.lider_id != self.no_id:
                self.lider_atual = resposta.lider_id   # Segue a dica de quem é o líder
            return

        # OK ou DIVERGENTE: veio de um líder
        if resposta.termo < self.termo:
            return   # Líder de um termo antigo
        if pedido.ultimo_indice != self._ultimo_indice() or pedido.ultimo_termo != self._ultimo_termo():
            return   # O log mudou enquanto a resposta viajava: o próximo Fetch refaz o pedido

        if self.papel != SEGUIDOR:
            self._tornar_seguidor(resposta.termo, alvo)
        else:
            self._definir_lider(alvo)
            self._reiniciar_temporizador()   # Fetch bem-sucedido: o líder está vivo

        if resposta.status == pb.DIVERGENTE:
            x = resposta.indice_consistente
            removidas = self.log[x:]
            self.log = self.log[:x]
            self.disco.truncar_log(x)
            for e in removidas:
                self.mostrar(f'TRUNCADO índice={e.indice} termo={e.termo} {e.chave}={e.valor} (não comitado, diverge do líder)')
            return

        if resposta.entradas:
            novas = [pb.Entrada(indice=e.indice, termo=e.termo, chave=e.chave, valor=e.valor) for e in resposta.entradas]
            self.log.extend(novas)
            self.disco.anexar_entradas(novas)
            for e in novas:
                self.mostrar(f'Recebida via Fetch índice={e.indice} termo={e.termo} {e.chave}={e.valor} '
                             f'(ainda NÃO comitada, invisível para leitura)')

        novo_commit = min(resposta.commit_index, self._ultimo_indice())
        if novo_commit > self.commit_index:
            self._aplicar_commit(novo_commit)

    # --- Gravação (cliente -> líder) ---

    def gravar(self, pedido):
        with self.lock:
            if self.papel != LIDER:
                return pb.GravarResponse(
                    status=pb.NAO_LIDER, lider_id=self.lider_atual, termo=self.termo,
                    mensagem=f'Nó {self.no_id} é {NOME_PAPEL[self.papel]}',
                )

            ativos = 1 + len(self._ativas())
            if ativos < QUORUM:
                self.mostrar(f'Escrita {pedido.chave}={pedido.valor} RECUSADA: sem quórum ({ativos} de {len(NOS)} nós ativos)')
                return pb.GravarResponse(
                    status=pb.SEM_QUORUM, lider_id=self.no_id, termo=self.termo,
                    mensagem=f'Sem quórum: apenas {ativos} de {len(NOS)} nós ativos (mínimo {QUORUM})',
                )

            entrada = pb.Entrada(indice=self._ultimo_indice() + 1, termo=self.termo,
                                 chave=pedido.chave, valor=pedido.valor)
            self.log.append(entrada)
            self.disco.anexar_entradas([entrada])
            self.mostrar(f'Gravação recebida: índice={entrada.indice} termo={entrada.termo} '
                         f'{entrada.chave}={entrada.valor} (no log, aguardando quórum)')

            if pedido.simular_queda:
                self.mostrar('QUEDA SIMULADA: encerrando antes de replicar a entrada')
                os._exit(1)

            termo_escrita = self.termo
            self.cond.wait_for(
                lambda: self.commit_index >= entrada.indice or self.papel != LIDER or self.termo != termo_escrita,
                timeout=TIMEOUT_COMMIT,
            )

            if self.commit_index >= entrada.indice and self._termo_em(entrada.indice) == termo_escrita:
                return pb.GravarResponse(
                    status=pb.OK, lider_id=self.no_id, indice=entrada.indice, termo=termo_escrita,
                    mensagem=f'Comitado no índice {entrada.indice}',
                )
            if self.papel != LIDER:
                return pb.GravarResponse(
                    status=pb.NAO_LIDER, lider_id=self.lider_atual, indice=entrada.indice, termo=termo_escrita,
                    mensagem='Perdeu a liderança antes do commit',
                )
            return pb.GravarResponse(
                status=pb.PENDENTE, lider_id=self.no_id, indice=entrada.indice, termo=termo_escrita,
                mensagem=f'Gravada no log (índice {entrada.indice}), mas não comitada em {TIMEOUT_COMMIT:.0f} s',
            )

    # --- Consulta, estado e depuração ---

    def consultar(self, pedido):
        registros = self.disco.consultar(pedido.chave or None)   # Só o banco final (dados comitados)
        with self.lock:
            estado = self._estado()
        return pb.ConsultarResponse(registros=registros, estado=estado)

    def obter_estado(self):
        with self.lock:
            return self._estado()

    def depurar(self, pedido):
        with self.lock:
            if pedido.acao == 'segurar_commit':
                self.commit_segurado = True
                self.mostrar('DEPURAÇÃO: commit SEGURADO (as entradas replicam, mas não são efetivadas)')
            elif pedido.acao == 'liberar_commit':
                self.commit_segurado = False
                self.mostrar('DEPURAÇÃO: commit LIBERADO')
                if self.papel == LIDER:
                    self._avancar_commit()
            return self._estado()

    # --- Exibição ---

    def loop_status(self):
        """Avisa mudanças nas réplicas ativas (líder), imprime o status e atualiza o título da janela."""
        proximo_status = 0.0
        while True:
            time.sleep(FETCH_INTERVALO)
            with self.lock:
                if self.papel == LIDER:
                    ativas = set(self._ativas())
                    for nid in sorted(ativas - self.replicas_ativas):
                        self.mostrar(f'Réplica Nó {nid} ATIVA (Fetch recebido)')
                    for nid in sorted(self.replicas_ativas - ativas):
                        self.mostrar(f'Réplica Nó {nid} INATIVA (sem Fetch há mais de {HEARTBEAT_TIMEOUT} s)')
                    if (1 + len(ativas) >= QUORUM) != (1 + len(self.replicas_ativas) >= QUORUM):
                        situacao = 'RESTABELECIDO' if 1 + len(ativas) >= QUORUM else 'PERDIDO: escritas bloqueadas'
                        self.mostrar(f'Quórum {situacao} ({1 + len(ativas)} de {len(NOS)} nós ativos)')
                    self.replicas_ativas = ativas

                lider = f'Nó {self.lider_atual}' if self.lider_atual else '?'
                linha = (f'{NOME_PAPEL[self.papel]} | termo={self.termo} | commit={self.commit_index} '
                         f'| log={self._ultimo_indice()} | líder={lider}')
                if self.papel == LIDER:
                    linha += f' | ativas={sorted(self.replicas_ativas)}'

            self.atualizar_titulo(f'Nó {self.no_id} - {linha}')
            if time.monotonic() >= proximo_status:
                self.mostrar(linha)
                proximo_status = time.monotonic() + STATUS_INTERVALO

    def atualizar_titulo(self, titulo):
        if titulo == self.ultimo_titulo or os.name != 'nt':
            return
        self.ultimo_titulo = titulo
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW(titulo)
        except Exception:
            pass

# --- Servicers gRPC ---

class ClusterServicer(pb_grpc.ClusterServicer):
    def __init__(self, no):
        self.no = no

    def RequestVote(self, request, context):
        return self.no.receber_pedido_voto(request)

    def Fetch(self, request, context):
        return self.no.atender_fetch(request)

class ArmazenamentoServicer(pb_grpc.ArmazenamentoServicer):
    def __init__(self, no):
        self.no = no

    def Gravar(self, request, context):
        return self.no.gravar(request)

    def Consultar(self, request, context):
        return self.no.consultar(request)

    def ObterEstado(self, request, context):
        return self.no.obter_estado()

    def Depurar(self, request, context):
        return self.no.depurar(request)

# --- Inicialização ---

def iniciar(no_id):
    no = NoCluster(no_id)

    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_ClusterServicer_to_server(ClusterServicer(no), servidor)
    pb_grpc.add_ArmazenamentoServicer_to_server(ArmazenamentoServicer(no), servidor)
    try:
        servidor.add_insecure_port(NOS[no_id])
    except RuntimeError:
        print(f'[Nó {no_id}] Não foi possível abrir {NOS[no_id]} (porta em uso?)')
        sys.exit(1)
    servidor.start()
    no.mostrar(f'Servidor gRPC ouvindo em {NOS[no_id]}')

    threading.Thread(target=no.loop_fetch, daemon=True).start()
    threading.Thread(target=no.loop_status, daemon=True).start()

    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        no.mostrar('Encerrando...')
        servidor.stop(0)

if __name__ == '__main__':
    if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) not in NOS:
        print(f'Uso: python no_cluster/no_cluster.py <id>   (id: {", ".join(map(str, NOS))})')
        sys.exit(1)
    iniciar(int(sys.argv[1]))
