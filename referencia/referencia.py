"""Serviço de referência (Partes 3 e 4).
Só conversa com os servidores (nunca com os bots). Ele:
  1) dá o RANK de cada servidor (sem repetir nome)
  2) guarda e fornece a LISTA de servidores disponíveis
  3) recebe o HEARTBEAT: quem ficar muito tempo sem avisar sai da lista
  (Parte 4: a hora não vem mais no heartbeat; quem fornece a hora é o coordenador eleito)
"""
import os
import time
from datetime import datetime

import msgpack
import zmq

PORTA = 5559
TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "30"))   # segundos sem heartbeat -> removido

ranks = {}          # nome -> rank (um nome nunca recebe dois ranks)
ultimo_hb = {}      # nome -> instante do último sinal de vida (= servidores disponíveis)
proximo_rank = 1
clock = 0           # relógio lógico do próprio serviço


# ---------------------------------------------------------------- utilidades
def hora(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]


def log(direcao: str, msg: dict, extra: str = "") -> None:
    print(
        f"[referencia] {direcao:<8} tipo={msg.get('tipo')} ts={hora(msg['ts'])} "
        f"clock={msg.get('clock')} dados={msg.get('dados')} {extra}".rstrip(),
        flush=True,
    )


def mensagem(tipo: str, dados: dict, **campos) -> dict:
    global clock
    clock += 1                                   # incrementa ANTES de enviar
    return {"tipo": tipo, "ts": time.time(), "clock": clock, "dados": dados, **campos}


# ------------------------------------------------------------------- estado
def registrar(nome: str) -> int:
    """Cadastra o servidor (se for novo) e marca que ele está vivo agora."""
    global proximo_rank
    if nome not in ranks:
        ranks[nome] = proximo_rank
        proximo_rank += 1
    if nome not in ultimo_hb:
        print(f"[referencia] ENTROU   {nome} (rank {ranks[nome]})", flush=True)
    ultimo_hb[nome] = time.time()
    return ranks[nome]


def remover_expirados() -> None:
    agora = time.time()
    for nome in [n for n, t in ultimo_hb.items() if agora - t > TIMEOUT]:
        del ultimo_hb[nome]
        print(f"[referencia] REMOVIDO {nome}: sem heartbeat há mais de {TIMEOUT:.0f}s", flush=True)


# ----------------------------------------------------------------- handlers
def h_rank(d):
    nome = d.get("nome", "")
    if not nome:
        return "erro", "nome obrigatório", {}
    return "ok", "", {"rank": registrar(nome)}


def h_list(_d):
    remover_expirados()
    lista = [{"nome": n, "rank": ranks[n]} for n in sorted(ultimo_hb, key=ranks.get)]
    return "ok", "", {"servidores": lista}


def h_heartbeat(d):
    nome = d.get("nome", "")
    if not nome:
        return "erro", "nome obrigatório", {}
    registrar(nome)                              # renova (ou readmite) o servidor
    return "ok", "", {}                          # só confirma: OK


HANDLERS = {"rank": h_rank, "list": h_list, "heartbeat": h_heartbeat}

# ------------------------------------------------------------------- socket
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.bind(f"tcp://*:{PORTA}")
poller = zmq.Poller()
poller.register(rep, zmq.POLLIN)

print(f"[referencia] pronto na porta {PORTA} (timeout do heartbeat: {TIMEOUT:.0f}s)", flush=True)

# --------------------------------------------------------------------- loop
while True:
    remover_expirados()                          # roda mesmo que ninguém pergunte nada
    if not poller.poll(1000):                    # espera até 1 s por uma mensagem
        continue

    req = msgpack.unpackb(rep.recv())
    clock = max(clock, req.get("clock") or 0)    # ao receber: máximo(meu, recebido)
    log("RECEBIDO", req)

    handler = HANDLERS.get(req.get("tipo"))
    try:
        if handler is None:
            status, texto, dados = "erro", f"tipo desconhecido: {req.get('tipo')}", {}
        else:
            status, texto, dados = handler(req.get("dados") or {})
    except Exception as e:
        status, texto, dados = "erro", f"erro interno: {e}", {}

    resp = mensagem("resposta", dados, status=status, msg=texto)
    rep.send(msgpack.packb(resp))
    log("ENVIADO", resp, f"status={status} {texto}")
