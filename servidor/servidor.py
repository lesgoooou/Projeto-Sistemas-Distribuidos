"""Servidor - Partes 1 a 5: login, canais, publicação, relógios, rank, heartbeat, eleição e REPLICAÇÃO.
Cada servidor tem o seu próprio banco SQLite, e todos os bancos convergem para o mesmo conteúdo:
cada registro novo é replicado aos outros servidores (tópico "replica") e quem ficou fora recupera o que perdeu."""
import os
import random
import re
import sqlite3
import time
import uuid
from datetime import datetime

import msgpack
import zmq

NAME = os.getenv("SERVER_NAME", "server1")
BROKER = os.getenv("BROKER_BACKEND", "tcp://broker:5556")
PROXY = os.getenv("PROXY_XSUB", "tcp://proxy:5557")      # onde o servidor publica
DATA_DIR = os.getenv("DATA_DIR", "/data")
REF = os.getenv("REF_ADDR", "tcp://referencia:5559")          # serviço de referência
CLOCK_SKEW = float(os.getenv("CLOCK_SKEW", "0"))              # simula relógio físico errado (teste)
PROXY_XPUB = os.getenv("PROXY_XPUB", "tcp://proxy:5558")      # de onde o servidor escuta o tópico "servers"
PEER_BIND = os.getenv("PEER_BIND", "tcp://*:6000")             # porta em que este servidor atende outros servidores
PEER_ADDR = os.getenv("PEER_ADDR", "tcp://{nome}:6000")        # endereço de um servidor (nome = nome do serviço no compose)
PEER_TIMEOUT_MS = 2000                                         # quanto esperar uma resposta de outro servidor
HEARTBEAT_A_CADA = int(os.getenv("HEARTBEAT_A_CADA", "10"))    # mensagens de clientes entre heartbeats
SYNC_A_CADA = int(os.getenv("SYNC_A_CADA", "15"))              # mensagens de clientes entre sincronizações do relógio
RECONCILIAR_A_CADA = int(os.getenv("RECONCILIAR_A_CADA", "100"))  # mensagens de clientes entre conferências de dados
TOPICO_SERVIDORES = "servers"                                  # tópico reservado: anúncio do coordenador
TOPICO_REPLICA = "replica"                                     # tópico reservado: replicação de registros
TOPICO_STATUS = "status"                                       # tópico reservado: estado do servidor (só para o painel)
RESERVADOS = {TOPICO_SERVIDORES, TOPICO_REPLICA, TOPICO_STATUS}  # nomes que os bots não podem usar como canal

# ---- EXTRAS para a apresentação (fora do enunciado). Com os valores padrão, ficam desligados. ----
STATUS_INTERVALO = float(os.getenv("STATUS_INTERVALO", "0"))   # >0: publica o estado a cada N segundos (alimenta o painel)
FALHA_MIN = int(os.getenv("FALHA_MIN", "0"))                   # >0: o servidor "cai" depois de N mensagens de clientes,
FALHA_MAX = int(os.getenv("FALHA_MAX", "0"))                   #     com N sorteado entre FALHA_MIN e FALHA_MAX
FALHA_PAUSA = float(os.getenv("FALHA_PAUSA", "10"))            # segundos que ele fica fora do ar antes de voltar
FALHA_APOS = random.randint(FALHA_MIN, max(FALHA_MIN, FALHA_MAX)) if FALHA_MIN > 0 else 0

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
    dados = str(msg.get("dados"))
    if len(dados) > 400:                      # sincronizações carregam listas enormes
        dados = dados[:400] + f"... (+{len(dados) - 400} caracteres)"
    print(
        f"[{NAME}] {direcao:<8} tipo={msg.get('tipo')} ts={hora(msg['ts'])} "
        f"clock={msg.get('clock')} dados={dados} {extra}".rstrip(),
        flush=True,
    )


def mensagem(tipo: str, dados: dict, **campos) -> dict:
    """Envelope padrão das mensagens (obrigatório: timestamp de envio)."""
    return {"tipo": tipo, "ts": agora(), "clock": tick(), "dados": dados, **campos}


# ------------------------------------------------------------- persistência
os.makedirs(DATA_DIR, exist_ok=True)
MARCADOR_QUEDA = os.path.join(DATA_DIR, ".queda")
if os.path.exists(MARCADOR_QUEDA):       # o processo anterior "caiu" de propósito: fica fora do ar por um tempo
    os.remove(MARCADOR_QUEDA)
    print(f"[{NAME}] FALHA    reiniciado após uma queda simulada; volta em {FALHA_PAUSA:.0f}s", flush=True)
    time.sleep(FALHA_PAUSA)
db = sqlite3.connect(os.path.join(DATA_DIR, f"{NAME}.db"))

# Todo registro tem: id único, origem (servidor que o criou) e clock lógico de quando foi criado.
COLUNAS = {
    "logins":    ("id", "origem", "clock", "usuario", "ts"),
    "canais":    ("id", "origem", "clock", "nome", "ts"),
    "mensagens": ("id", "origem", "clock", "canal", "usuario", "texto", "ts_envio", "ts_publicacao"),
}

# Bancos criados nas Partes 1 a 4 não tinham "id": as tabelas antigas são renomeadas (nada é apagado).
for tabela in COLUNAS:
    atuais = [r[1] for r in db.execute(f"PRAGMA table_info({tabela})")]
    if atuais and "id" not in atuais:
        db.execute(f"ALTER TABLE {tabela} RENAME TO {tabela}_antigo")
        print(f"[{NAME}] BANCO    tabela antiga '{tabela}' renomeada para '{tabela}_antigo'", flush=True)
db.executescript(
    """
    CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, origem TEXT, clock INTEGER,
                                       usuario TEXT, ts REAL);
    CREATE TABLE IF NOT EXISTS canais (id TEXT PRIMARY KEY, origem TEXT, clock INTEGER,
                                       nome TEXT, ts REAL);
    CREATE TABLE IF NOT EXISTS mensagens (id TEXT PRIMARY KEY, origem TEXT, clock INTEGER, canal TEXT,
                                          usuario TEXT, texto TEXT, ts_envio REAL, ts_publicacao REAL);
    """
)
db.commit()


def estatisticas() -> str:
    return " ".join(f"{t}={db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]}" for t in COLUNAS)


# -------------------------------------------------------------- replicação
def gravar(tabela: str, reg: dict) -> bool:
    """Grava o registro se ainda não existir (INSERT OR IGNORE pelo id). Devolve True se era novo.
    Como a gravação é idempotente, receber o mesmo registro duas vezes não causa problema."""
    cols = COLUNAS[tabela]
    cur = db.execute(
        f"INSERT OR IGNORE INTO {tabela} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
        [reg.get(c) for c in cols],
    )
    db.commit()
    return cur.rowcount == 1


def replicar(tabela: str, reg: dict) -> None:
    """Envia um registro novo a todos os outros servidores (tópico 'replica')."""
    msg = mensagem("replica", {"tabela": tabela, "registro": reg})
    pub_sock.send_multipart([TOPICO_REPLICA.encode(), msgpack.packb(msg)])
    log("PUBLICADO", msg, f"-> tópico '{TOPICO_REPLICA}'")


def criar_registro(tabela: str, **campos) -> dict:
    """Cria um registro NOVO neste servidor: grava e replica."""
    reg = {"id": campos.pop("id", None) or f"{NAME}-{uuid.uuid4().hex[:10]}",
           "origem": NAME, "clock": clock, **campos}
    if gravar(tabela, reg):
        replicar(tabela, reg)
    return reg


def aplicar_replica(tabela, reg) -> bool:
    """Grava um registro que veio de OUTRO servidor (não republica, para não gerar eco)."""
    if tabela not in COLUNAS or not isinstance(reg, dict) or not reg.get("id"):
        return False
    return gravar(tabela, reg)


def ids_locais() -> dict:
    return {t: [r[0] for r in db.execute(f"SELECT id FROM {t}")] for t in COLUNAS}


def registros_que_faltam(ids_do_outro: dict) -> dict:
    """Registros que EU tenho e o outro servidor (que mandou a lista de ids dele) não tem."""
    resultado = {}
    for t, cols in COLUNAS.items():
        conhecidos = set(ids_do_outro.get(t, []))
        linhas = db.execute(f"SELECT {', '.join(cols)} FROM {t}")
        resultado[t] = [dict(zip(cols, l)) for l in linhas if l[0] not in conhecidos]
    return resultado


def ressincronizar(nome: str, timeout_ms: int = 3000) -> bool:
    """Anti-entropia: mando a lista de ids que tenho e gravo o que o servidor 'nome' tiver a mais."""
    resp = pedir_peer(nome, "sincronizar", {"ids": ids_locais()}, timeout_ms=timeout_ms)
    if not (resp and resp.get("status") == "ok"):
        return False
    novos = sum(aplicar_replica(t, r) for t, regs in resp["dados"]["registros"].items() for r in regs)
    print(f"[{NAME}] REPLICA  sincronizado com {nome}: +{novos} registros | {estatisticas()}", flush=True)
    return True


def sincronizar_inicio() -> None:
    """Ao entrar (ou voltar), copia de um servidor vivo tudo o que perdeu enquanto esteve fora."""
    lista = pedir_ref("list")
    if not (lista and lista.get("status") == "ok"):
        return
    for x in lista["dados"]["servidores"]:
        if x["nome"] != NAME and ressincronizar(x["nome"], timeout_ms=10000):
            return                                  # um servidor vivo basta


def reconciliar() -> None:
    """Conferência periódica com um servidor qualquer: conserta o que uma publicação perdida deixou faltando."""
    lista = pedir_ref("list")
    if lista and lista.get("status") == "ok":
        outros = [x["nome"] for x in lista["dados"]["servidores"] if x["nome"] != NAME]
        if outros:
            ressincronizar(random.choice(outros))


# ----------------------------------------------------------------- handlers
# Cada handler recebe o dict "dados" e devolve (status, texto, dados_da_resposta)
def h_login(d):
    usuario = d.get("usuario", "")
    if not RE_USUARIO.match(usuario):
        return "erro", "usuário inválido (letras, números, _ ou -; até 32)", {}
    criar_registro("logins", usuario=usuario, ts=agora())      # grava o login + timestamp e replica
    return "ok", "login realizado", {}


def h_criar_canal(d):
    nome = d.get("canal", "")
    if not RE_CANAL.match(nome):
        return "erro", "nome de canal inválido (minúsculas, números, _ ou -)", {}
    if nome in RESERVADOS:
        return "erro", "nome de canal reservado", {}
    if db.execute("SELECT 1 FROM canais WHERE nome = ?", (nome,)).fetchone():
        return "erro", "canal já existe", {}
    # id derivado do nome: se dois servidores criarem o mesmo canal ao mesmo tempo, o registro é o mesmo
    criar_registro("canais", id=f"canal:{nome}", nome=nome, ts=agora())
    return "ok", "canal criado", {}


def h_listar(_d):
    canais = [r[0] for r in db.execute("SELECT nome FROM canais ORDER BY nome")]
    return "ok", "", {"canais": canais}


def h_publicar(d):
    canal, usuario, texto = d.get("canal"), d.get("usuario"), d.get("mensagem")
    if not (canal and usuario and texto):
        return "erro", "campos obrigatórios: canal, usuario, mensagem", {}
    if canal in RESERVADOS:
        return "erro", "nome de canal reservado", {}
    # Se a réplica do canal ainda não chegou aqui (milissegundos de atraso), registra e replica o canal.
    if not db.execute("SELECT 1 FROM canais WHERE nome = ?", (canal,)).fetchone():
        criar_registro("canais", id=f"canal:{canal}", nome=canal, ts=agora())
    pub = mensagem("publicacao", {"canal": canal, "usuario": usuario, "mensagem": texto})
    # 1) grava em disco e replica para os outros servidores
    criar_registro("mensagens", canal=canal, usuario=usuario, texto=texto,
                   ts_envio=d.get("_ts_envio"), ts_publicacao=pub["ts"], clock=pub["clock"])
    # 2) publica no canal (tópico = nome do canal) para os bots inscritos
    pub_sock.send_multipart([canal.encode(), msgpack.packb(pub)])
    log("PUBLICADO", pub, f"-> tópico '{canal}'")
    return "ok", "mensagem publicada", {}


def h_historico(d):
    """Devolve as últimas mensagens de um canal. Como todos os servidores têm todos os dados,
    qualquer um responde com o histórico completo."""
    canal = d.get("canal")
    limite = max(1, min(int(d.get("limite") or 50), 200))
    total = db.execute("SELECT COUNT(*) FROM mensagens WHERE canal = ?", (canal,)).fetchone()[0]
    linhas = db.execute(
        "SELECT usuario, texto, ts_publicacao, clock, origem FROM mensagens WHERE canal = ? "
        "ORDER BY clock DESC, origem DESC, id DESC LIMIT ?", (canal, limite)).fetchall()
    msgs = [{"usuario": u, "texto": t, "ts": ts, "clock": c, "origem": o}
            for (u, t, ts, c, o) in reversed(linhas)]               # do mais antigo ao mais novo
    return "ok", "", {"canal": canal, "total": total, "mensagens": msgs}


HANDLERS = {
    "login": h_login,
    "criar_canal": h_criar_canal,
    "listar": h_listar,
    "publicar": h_publicar,
    "historico": h_historico,
}

# ------------------------------------------------------------------- sockets
ctx = zmq.Context()
coordenador = None    # nome do servidor eleito coordenador (None = ainda não sabemos)
ranks_vistos = {}     # nome -> rank, atualizado a cada lista recebida da referência


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


# ------------------------------------------------- conversa entre servidores (Parte 4)
def pedir_peer(nome, tipo, dados=None, timeout_ms=PEER_TIMEOUT_MS):
    """REQ/REP com outro servidor. Devolve a resposta, ou None se ele não responder."""
    msg = mensagem(tipo, dados or {})
    log("ENVIADO", msg, f"-> {nome}")
    sock = ctx.socket(zmq.REQ)           # um socket novo por conversa: não fica "preso" se falhar
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.connect(PEER_ADDR.format(nome=nome))
        sock.send(msgpack.packb(msg))
        resp = msgpack.unpackb(sock.recv())
    except zmq.ZMQError:                 # inclui timeout (zmq.Again) e nome que não resolve
        print(f"[{NAME}] ERRO     {nome} não respondeu ({tipo})", flush=True)
        return None
    finally:
        sock.close()
    atualizar_clock(resp.get("clock"))
    log("RECEBIDO", resp, f"<- {nome}")
    return resp


def anunciar_coordenador():
    """Eu venci a eleição: aviso todos os servidores publicando no tópico 'servers'."""
    global coordenador
    coordenador = NAME
    msg = mensagem("coordenador", {"coordenador": NAME})
    pub_sock.send_multipart([TOPICO_SERVIDORES.encode(), msgpack.packb(msg)])
    log("PUBLICADO", msg, f"-> tópico '{TOPICO_SERVIDORES}'")
    print(f"[{NAME}] COORDENADOR agora: {NAME}", flush=True)


def eleicao():
    """Eleição pelo rank: pergunto aos servidores de rank MAIOR que o meu.
    Se algum responder OK, ele assume a eleição. Se ninguém responder, eu sou o coordenador."""
    print(f"[{NAME}] ELEICAO  iniciada (meu rank={RANK})", flush=True)
    lista = pedir_ref("list")
    if not (lista and lista.get("status") == "ok"):
        print(f"[{NAME}] ELEICAO  adiada: referência indisponível", flush=True)
        return
    ranks_vistos.update({x["nome"]: x["rank"] for x in lista["dados"]["servidores"]})
    maiores = [x["nome"] for x in lista["dados"]["servidores"] if x["rank"] > RANK]
    alguem_respondeu = False
    for nome in maiores:
        resp = pedir_peer(nome, "eleicao")
        if resp and resp.get("status") == "ok":
            alguem_respondeu = True
    if alguem_respondeu:
        print(f"[{NAME}] ELEICAO  um servidor de rank maior respondeu OK; aguardo o anúncio dele", flush=True)
    else:
        print(f"[{NAME}] ELEICAO  ninguém de rank maior respondeu: eu sou o coordenador", flush=True)
        anunciar_coordenador()


def sincronizar_relogio():
    """Pede a hora correta ao coordenador e ajusta o relógio físico.
    Se o coordenador não responder, ele é considerado indisponível e começa uma eleição."""
    global offset
    if coordenador == NAME:
        print(f"[{NAME}] RELOGIO  sou o coordenador: meu relógio é a referência", flush=True)
        return
    if coordenador is None:
        eleicao()
        return
    t0 = time.time()
    resp = pedir_peer(coordenador, "relogio")
    t1 = time.time()
    if not (resp and resp.get("status") == "ok"):
        print(f"[{NAME}] COORDENADOR {coordenador} indisponível", flush=True)
        eleicao()
        return
    hora_ref = resp["dados"]["hora"] + (t1 - t0) / 2   # compensa o tempo de ida e volta
    antes = offset
    offset = hora_ref - t1
    print(f"[{NAME}] RELOGIO  ajuste de {offset - antes:+.3f}s com {coordenador} "
          f"(offset {antes:+.3f}s -> {offset:+.3f}s)", flush=True)


def heartbeat():
    """Avisa a referência que está vivo (a resposta não traz mais a hora)."""
    pedir_ref("heartbeat", {"nome": NAME})
    lista = pedir_ref("list")
    if lista and lista.get("status") == "ok":
        nomes = [x["nome"] for x in lista["dados"]["servidores"]]
        ranks_vistos.update({x["nome"]: x["rank"] for x in lista["dados"]["servidores"]})
        print(f"[{NAME}] SERVIDORES disponíveis: " +
              ", ".join(f"{x['nome']}(rank {x['rank']})" for x in lista["dados"]["servidores"]), flush=True)
        print(f"[{NAME}] DADOS    {estatisticas()}", flush=True)     # compare entre os servidores
        if coordenador and coordenador != NAME and coordenador not in nomes:
            print(f"[{NAME}] COORDENADOR {coordenador} saiu da lista de servidores", flush=True)
            eleicao()


def outro_servidor_vivo() -> bool:
    """EXTRA: confere (perguntando direto a cada servidor) se existe outro servidor no ar."""
    lista = pedir_ref("list")
    if not (lista and lista.get("status") == "ok"):
        return False
    return any(pedir_peer(x["nome"], "relogio") is not None
               for x in lista["dados"]["servidores"] if x["nome"] != NAME)


def publicar_status():
    """EXTRA: informa ao painel o estado deste servidor (não é usado pelo sistema)."""
    contagens = {t: db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in COLUNAS}
    msg = mensagem("status", {"nome": NAME, "rank": RANK, "coordenador": coordenador,
                              "offset": offset, **contagens})
    pub_sock.send_multipart([TOPICO_STATUS.encode(), msgpack.packb(msg)])


# ----------------------------------------------------------- inicialização
RANK = obter_rank()    # pedido ANTES de conectar ao broker, para não receber trabalho sem estar pronto
print(f"[{NAME}] rank={RANK}", flush=True)

peer_rep = ctx.socket(zmq.REP)    # atende outros servidores: eleição e relógio
peer_rep.bind(PEER_BIND)

pub_sock = ctx.socket(zmq.PUB)    # publica no proxy (entrada XSUB)
pub_sock.connect(PROXY)

sub = ctx.socket(zmq.SUB)         # escuta o tópico "servers" no proxy (saída XPUB)
sub.connect(PROXY_XPUB)
sub.setsockopt(zmq.SUBSCRIBE, TOPICO_SERVIDORES.encode())
sub.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICA.encode())      # registros replicados pelos outros servidores

time.sleep(1)    # dá tempo das conexões PUB/SUB se estabelecerem antes do primeiro anúncio
eleicao()        # ao entrar, o servidor descobre (ou assume) o coordenador
sincronizar_inicio()   # e copia o que perdeu enquanto esteve fora (antes de atender os bots)

rep = ctx.socket(zmq.REP)         # recebe as requisições dos bots, vindas do broker
rep.connect(BROKER)

print(f"[{NAME}] pronto. broker={BROKER} proxy={PROXY} ref={REF}", flush=True)


# ------------------------------------------------------------------ atendimento
def atender_peer():
    """Outro servidor pediu 'eleicao' ou 'relogio'."""
    req = msgpack.unpackb(peer_rep.recv())
    atualizar_clock(req.get("clock"))
    log("RECEBIDO", req, "<- servidor")
    depois = None
    if req.get("tipo") == "eleicao":
        resp = mensagem("resposta", {}, status="ok", msg="OK")      # REP: OK
        depois = eleicao                                            # e eu assumo a eleição
    elif req.get("tipo") == "relogio":
        if coordenador == NAME:
            resp = mensagem("resposta", {"hora": agora()}, status="ok", msg="")   # REP: hora correta
        else:
            resp = mensagem("resposta", {}, status="erro", msg="não sou o coordenador")
    elif req.get("tipo") == "sincronizar":
        faltantes = registros_que_faltam((req.get("dados") or {}).get("ids") or {})
        resp = mensagem("resposta", {"registros": faltantes}, status="ok", msg="")
    else:
        resp = mensagem("resposta", {}, status="erro", msg=f"tipo desconhecido: {req.get('tipo')}")
    peer_rep.send(msgpack.packb(resp))
    log("ENVIADO", resp, f"status={resp['status']} {resp['msg']}")
    if depois:
        depois()


def atender_replica(raw):
    """Chegou um registro replicado por outro servidor: grava se ainda não tinha."""
    msg = msgpack.unpackb(raw)
    atualizar_clock(msg.get("clock"))
    dados = msg.get("dados") or {}
    reg = dados.get("registro") or {}
    if reg.get("origem") == NAME:                # eco do que eu mesmo publiquei
        return
    novo = aplicar_replica(dados.get("tabela"), reg)
    log("RECEBIDO", msg, f"<- tópico '{TOPICO_REPLICA}' ({'novo' if novo else 'já existia'})")


def atender_anuncio():
    """Chegou uma publicação em um tópico dos servidores: 'replica' ou 'servers' (novo coordenador)."""
    global coordenador
    topico, raw = sub.recv_multipart()
    if topico == TOPICO_REPLICA.encode():
        atender_replica(raw)
        return
    if topico != TOPICO_SERVIDORES.encode():     # assinatura é por prefixo: ignora "servers_x" etc.
        return
    msg = msgpack.unpackb(raw)
    atualizar_clock(msg.get("clock"))
    log("RECEBIDO", msg, f"<- tópico '{TOPICO_SERVIDORES}'")
    novo = msg["dados"]["coordenador"]
    if novo == NAME:        # eco do meu próprio anúncio (o proxy devolve para mim também): já registrei
        return
    rank_novo = ranks_vistos.get(novo)
    if rank_novo is not None and rank_novo < RANK:
        # Se um servidor de rank MENOR se declarou coordenador mas eu (maior) estou vivo,
        # não aceito: assumo a eleição (isso desfaz duas eleições simultâneas).
        print(f"[{NAME}] ANUNCIO  de {novo} (rank {rank_novo}) não aceito: meu rank ({RANK}) é maior", flush=True)
        eleicao()
        return
    if novo != coordenador:
        coordenador = novo
        print(f"[{NAME}] COORDENADOR agora: {novo}", flush=True)


recebidas = 0


def atender_cliente():
    """Requisição de um bot, vinda do broker."""
    global recebidas, FALHA_APOS
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
    if recebidas % SYNC_A_CADA == 0:           # a cada 15 mensagens de clientes
        sincronizar_relogio()
    if recebidas % RECONCILIAR_A_CADA == 0:    # a cada 100 mensagens de clientes
        reconciliar()
    if FALHA_APOS and recebidas >= FALHA_APOS:  # EXTRA: queda simulada (o Docker reinicia o container)
        if outro_servidor_vivo():
            open(MARCADOR_QUEDA, "w").close()
            print(f"[{NAME}] FALHA    queda simulada após {recebidas} mensagens de clientes", flush=True)
            os._exit(1)
        FALHA_APOS = recebidas + 10             # nunca derruba o último servidor no ar: tenta de novo depois
        print(f"[{NAME}] FALHA    queda adiada: não há outro servidor no ar", flush=True)


# --------------------------------------------------------------------- loop
poller = zmq.Poller()
for sock in (peer_rep, sub, rep):
    poller.register(sock, zmq.POLLIN)

ultimo_status = 0.0
while True:
    # espera qualquer um dos 3 sockets (com o painel ligado, acorda a cada 1 s para publicar o status)
    eventos = dict(poller.poll(1000 if STATUS_INTERVALO > 0 else None))
    if STATUS_INTERVALO > 0 and time.time() - ultimo_status >= STATUS_INTERVALO:
        publicar_status()
        ultimo_status = time.time()
    if peer_rep in eventos:
        atender_peer()
    if sub in eventos:
        atender_anuncio()
    if rep in eventos:
        atender_cliente()
