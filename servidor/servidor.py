"""Servidor - Partes 1 a 3: login, canais, publicação, relógio lógico, rank e heartbeat.
Cada servidor tem o seu próprio banco SQLite (não compartilhado com os outros)."""
import os
import re
import sqlite3
import time
from datetime import datetime

import msgpack
import zmq

NAME = os.getenv("SERVER_NAME", "server1")
BROKER = os.getenv("BROKER_BACKEND", "tcp://broker:5556")
PROXY = os.getenv("PROXY_XSUB", "tcp://proxy:5557")      # onde o servidor publica
DATA_DIR = os.getenv("DATA_DIR", "/data")
REF = os.getenv("REF_ADDR", "tcp://referencia:5559")          # serviço de referência
CLOCK_SKEW = float(os.getenv("CLOCK_SKEW", "0"))              # simula relógio físico errado (teste)
HEARTBEAT_A_CADA = 10                                          # mensagens de clientes entre heartbeats

RE_USUARIO = re.compile(r"^[A-Za-z0-9_-]{1,32}$")   # nomes de usuário permitidos
RE_CANAL = re.compile(r"^[a-z0-9_-]{1,32}$")        # nomes de canal permitidos


# ---------------------------------------------------------------- utilidades
# ----------------------------------------------------------------- relógios
offset = CLOCK_SKEW   # relógio FÍSICO = hora local + offset (o heartbeat ajusta o offset)
clock = 0             # relógio LÓGICO (contador)


def agora() -> float:
    return time.time() + offset


def tick() -> int:
    """Antes de enviar qualquer mensagem: incrementa o contador."""
    global clock
    clock += 1
    return clock


def atualizar_clock(recebido) -> None:
    """Ao receber uma mensagem: novo valor = máximo(meu contador, o recebido)."""
    global clock
    clock = max(clock, recebido or 0)


def hora(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]


def log(direcao: str, msg: dict, extra: str = "") -> None:
    print(
        f"[{NAME}] {direcao:<8} tipo={msg.get('tipo')} ts={hora(msg['ts'])} "
        f"clock={msg.get('clock')} dados={msg.get('dados')} {extra}".rstrip(),
        flush=True,
    )


def mensagem(tipo: str, dados: dict, **campos) -> dict:
    """Envelope padrão das mensagens (obrigatório: timestamp de envio)."""
    return {"tipo": tipo, "ts": agora(), "clock": tick(), "dados": dados, **campos}


# ------------------------------------------------------------- persistência
os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, f"{NAME}.db"))
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS logins (usuario TEXT, ts REAL);
    CREATE TABLE IF NOT EXISTS canais (nome TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS mensagens (canal TEXT, usuario TEXT, texto TEXT,
                                          ts_envio REAL, ts_publicacao REAL);
    """
)
db.commit()


# ----------------------------------------------------------------- handlers
# Cada handler recebe o dict "dados" e devolve (status, texto, dados_da_resposta)
def h_login(d):
    usuario = d.get("usuario", "")
    if not RE_USUARIO.match(usuario):
        return "erro", "usuário inválido (letras, números, _ ou -; até 32)", {}
    db.execute("INSERT INTO logins VALUES (?, ?)", (usuario, agora()))  # guarda o login + timestamp
    db.commit()
    return "ok", "login realizado", {}


def h_criar_canal(d):
    nome = d.get("canal", "")
    if not RE_CANAL.match(nome):
        return "erro", "nome de canal inválido (minúsculas, números, _ ou -)", {}
    if db.execute("SELECT 1 FROM canais WHERE nome = ?", (nome,)).fetchone():
        return "erro", "canal já existe", {}
    db.execute("INSERT INTO canais VALUES (?)", (nome,))
    db.commit()
    return "ok", "canal criado", {}


def h_listar(_d):
    canais = [r[0] for r in db.execute("SELECT nome FROM canais ORDER BY nome")]
    return "ok", "", {"canais": canais}


def h_publicar(d):
    canal, usuario, texto = d.get("canal"), d.get("usuario"), d.get("mensagem")
    if not (canal and usuario and texto):
        return "erro", "campos obrigatórios: canal, usuario, mensagem", {}
    # Com round-robin este servidor pode não conhecer o canal (foi criado em outro).
    # Até a Parte 5 (replicação), registramos o canal localmente.
    db.execute("INSERT OR IGNORE INTO canais VALUES (?)", (canal,))
    pub = mensagem("publicacao", {"canal": canal, "usuario": usuario, "mensagem": texto})
    # 1) grava em disco  2) publica no proxy, usando o nome do canal como tópico
    db.execute("INSERT INTO mensagens VALUES (?, ?, ?, ?, ?)",
               (canal, usuario, texto, d.get("_ts_envio"), pub["ts"]))
    db.commit()
    pub_sock.send_multipart([canal.encode(), msgpack.packb(pub)])
    log("PUBLICADO", pub, f"-> tópico '{canal}'")
    return "ok", "mensagem publicada", {}


HANDLERS = {
    "login": h_login,
    "criar_canal": h_criar_canal,
    "listar": h_listar,
    "publicar": h_publicar,
}

# ------------------------------------------------------------------- sockets
ctx = zmq.Context()


def novo_req_ref():
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 3000)
    sock.setsockopt(zmq.SNDTIMEO, 3000)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(REF)
    return sock


ref = novo_req_ref()   # fala com o serviço de referência


def pedir_ref(tipo, dados=None):
    """REQ/REP com a referência. Devolve a resposta, ou None se der timeout."""
    global ref
    msg = mensagem(tipo, dados or {})
    log("ENVIADO", msg, "-> referencia")
    try:
        ref.send(msgpack.packb(msg))
        resp = msgpack.unpackb(ref.recv())
    except zmq.Again:
        print(f"[{NAME}] ERRO     referencia não respondeu ({tipo}); recriando socket", flush=True)
        ref.close()
        ref = novo_req_ref()
        return None
    atualizar_clock(resp.get("clock"))
    log("RECEBIDO", resp, "<- referencia")
    return resp


def obter_rank():
    while True:                                   # repete até a referência responder
        resp = pedir_ref("rank", {"nome": NAME})
        if resp and resp.get("status") == "ok":
            return resp["dados"]["rank"]
        time.sleep(1)


def heartbeat():
    """Avisa que está vivo e aproveita a resposta para acertar o relógio físico."""
    global offset
    t0 = time.time()
    resp = pedir_ref("heartbeat", {"nome": NAME})
    t1 = time.time()
    if resp and resp.get("status") == "ok":
        # a hora da referência já "envelheceu" ~metade do tempo de ida e volta
        hora_ref = resp["dados"]["hora"] + (t1 - t0) / 2
        antes = offset
        offset = hora_ref - t1
        print(f"[{NAME}] RELOGIO  ajuste de {offset - antes:+.3f}s "
              f"(offset {antes:+.3f}s -> {offset:+.3f}s)", flush=True)
    lista = pedir_ref("list")
    if lista and lista.get("status") == "ok":
        txt = ", ".join(f"{x['nome']}(rank {x['rank']})" for x in lista["dados"]["servidores"])
        print(f"[{NAME}] SERVIDORES disponíveis: {txt}", flush=True)


# O rank é pedido ANTES de conectar ao broker, para não receber trabalho sem estar pronto
RANK = obter_rank()
print(f"[{NAME}] rank={RANK}", flush=True)

rep = ctx.socket(zmq.REP)       # recebe as requisições que vêm do broker
rep.connect(BROKER)

pub_sock = ctx.socket(zmq.PUB)  # publica no proxy (entrada XSUB)
pub_sock.connect(PROXY)

print(f"[{NAME}] pronto. broker={BROKER} proxy={PROXY} ref={REF}", flush=True)

# --------------------------------------------------------------------- loop
recebidas = 0
while True:
    req = msgpack.unpackb(rep.recv())
    atualizar_clock(req.get("clock"))          # ao receber: máximo(meu, recebido)
    log("RECEBIDO", req)

    handler = HANDLERS.get(req.get("tipo"))
    dados_req = dict(req.get("dados") or {})
    dados_req["_ts_envio"] = req.get("ts")     # timestamp de envio feito pelo bot
    try:
        if handler is None:
            status, texto, dados = "erro", f"tipo desconhecido: {req.get('tipo')}", {}
        else:
            status, texto, dados = handler(dados_req)
    except Exception as e:                     # REP sempre precisa responder
        status, texto, dados = "erro", f"erro interno: {e}", {}

    resp = mensagem("resposta", dados, status=status, msg=texto)
    rep.send(msgpack.packb(resp))
    log("ENVIADO", resp, f"status={status} {texto}")

    recebidas += 1
    if recebidas % HEARTBEAT_A_CADA == 0:      # a cada 10 mensagens de clientes
        heartbeat()
