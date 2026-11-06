"""Servidor - Parte 1: login, criar canal e listar canais.
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
DATA_DIR = os.getenv("DATA_DIR", "/data")

RE_USUARIO = re.compile(r"^[A-Za-z0-9_-]{1,32}$")   # nomes de usuário permitidos
RE_CANAL = re.compile(r"^[a-z0-9_-]{1,32}$")        # nomes de canal permitidos


# ---------------------------------------------------------------- utilidades
def agora() -> float:
    return time.time()


def hora(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]


def log(direcao: str, msg: dict, extra: str = "") -> None:
    print(
        f"[{NAME}] {direcao:<8} tipo={msg.get('tipo')} ts={hora(msg['ts'])} "
        f"dados={msg.get('dados')} {extra}".rstrip(),
        flush=True,
    )


def mensagem(tipo: str, dados: dict, **campos) -> dict:
    """Envelope padrão das mensagens (obrigatório: timestamp de envio)."""
    return {"tipo": tipo, "ts": agora(), "dados": dados, **campos}


# ------------------------------------------------------------- persistência
os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, f"{NAME}.db"))
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS logins (usuario TEXT, ts REAL);
    CREATE TABLE IF NOT EXISTS canais (nome TEXT PRIMARY KEY);
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


HANDLERS = {
    "login": h_login,
    "criar_canal": h_criar_canal,
    "listar": h_listar,
}

# ------------------------------------------------------------------- socket
ctx = zmq.Context()
rep = ctx.socket(zmq.REP)      # recebe as requisições que vêm do broker
rep.connect(BROKER)

print(f"[{NAME}] pronto. broker={BROKER}", flush=True)

# --------------------------------------------------------------------- loop
while True:
    req = msgpack.unpackb(rep.recv())      # 1) recebe e desserializa
    log("RECEBIDO", req)

    handler = HANDLERS.get(req.get("tipo"))
    try:
        if handler is None:
            status, texto, dados = "erro", f"tipo desconhecido: {req.get('tipo')}", {}
        else:
            status, texto, dados = handler(req.get("dados") or {})   # 2) processa
    except Exception as e:                 # REP sempre precisa responder
        status, texto, dados = "erro", f"erro interno: {e}", {}

    resp = mensagem("resposta", dados, status=status, msg=texto)
    rep.send(msgpack.packb(resp))          # 3) serializa e responde
    log("ENVIADO", resp, f"status={status} {texto}")
