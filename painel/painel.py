"""Painel de acompanhamento (EXTRA, fora do enunciado). SOMENTE LEITURA.
Assina o proxy (a mesma "rádio" que os bots ouvem) e mostra o estado do sistema numa página web.
Não fala com a referência, não envia nada aos servidores e não interfere no funcionamento do projeto."""
import json
import os
import threading
import time
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import msgpack
import zmq

PROXY = os.getenv("PROXY_XPUB", "tcp://proxy:5558")
PORTA = int(os.getenv("PORTA", "8080"))
SEM_SINAL_S = float(os.getenv("SEM_SINAL_S", "5"))     # segundos sem status -> servidor considerado fora do ar
PAGINA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")

lock = threading.Lock()
servidores = {}                  # nome -> dados do último status + estado ("ativo" | "caiu")
eventos = deque(maxlen=50)       # linha do tempo (mais novo primeiro)
mensagens = deque(maxlen=40)     # últimas mensagens de chat
canais = Counter()               # canal -> quantidade de mensagens vistas
ultimo_anuncio = None            # último coordenador anunciado no tópico "servers"
coordenador_atual = None         # para detectar troca de coordenador
total = 0
inicio = time.time()


def hora(t=None):
    return time.strftime("%H:%M:%S", time.localtime(t or time.time()))


def evento(texto, nivel="info"):          # chamar com o lock já adquirido
    eventos.appendleft({"hora": hora(), "texto": texto, "nivel": nivel})


def coordenador_vigente():
    """O coordenador que a maioria dos servidores ativos diz enxergar."""
    votos = Counter(s["visao"] for s in servidores.values() if s["estado"] == "ativo" and s.get("visao"))
    return votos.most_common(1)[0][0] if votos else ultimo_anuncio


def tratar_status(d):
    nome = d.get("nome")
    if not nome:
        return
    anterior = servidores.get(nome)
    if anterior is None:
        evento(f"{nome} entrou (rank {d.get('rank')})", "ok")
    elif anterior["estado"] == "caiu":
        evento(f"{nome} voltou", "ok")
    servidores[nome] = {
        "estado": "ativo", "visto": time.time(), "rank": d.get("rank"), "visao": d.get("coordenador"),
        "offset": d.get("offset") or 0, "logins": d.get("logins", 0), "canais": d.get("canais", 0),
        "mensagens": d.get("mensagens", 0),
    }


def ouvir():
    sub = zmq.Context().socket(zmq.SUB)
    sub.connect(PROXY)
    sub.setsockopt(zmq.SUBSCRIBE, b"")                 # todos os tópicos
    global ultimo_anuncio, total
    while True:
        partes = sub.recv_multipart()
        if len(partes) != 2 or partes[0] == b"replica":   # replicação não interessa ao painel
            continue
        topico, raw = partes
        try:
            msg = msgpack.unpackb(raw)
            dados = msg.get("dados") or {}
        except Exception:
            continue
        with lock:
            if topico == b"status":
                tratar_status(dados)
            elif topico == b"servers":
                ultimo_anuncio = dados.get("coordenador")
            else:                                          # qualquer outro tópico é um canal de chat
                total += 1
                nome_canal = topico.decode(errors="replace")
                canais[nome_canal] += 1
                mensagens.appendleft({"hora": hora(msg.get("ts")), "canal": nome_canal,
                                      "usuario": dados.get("usuario"), "texto": dados.get("mensagem")})


def vigiar():
    """Marca como 'caiu' quem ficou sem enviar status e detecta troca de coordenador."""
    global coordenador_atual
    while True:
        time.sleep(1)
        with lock:
            agora = time.time()
            for nome, s in servidores.items():
                if s["estado"] == "ativo" and agora - s["visto"] > SEM_SINAL_S:
                    s["estado"] = "caiu"
                    evento(f"{nome} sem sinal há {SEM_SINAL_S:.0f}s: fora do ar", "alerta")
            novo = coordenador_vigente()
            if novo and novo != coordenador_atual:
                evento(f"coordenador agora: {novo}", "info")
                coordenador_atual = novo


def estado():
    with lock:
        agora = time.time()
        coord = coordenador_vigente() or coordenador_atual
        lista = []
        for nome, s in sorted(servidores.items(), key=lambda kv: kv[1].get("rank") or 0):
            lista.append({
                "nome": nome, "rank": s["rank"], "estado": s["estado"],
                "coordenador": nome == coord and s["estado"] == "ativo", "visao": s["visao"],
                "offset_ms": round(s["offset"] * 1000), "logins": s["logins"], "canais": s["canais"],
                "mensagens": s["mensagens"], "visto_ha": round(agora - s["visto"], 1),
            })
        ativos = [x["mensagens"] for x in lista if x["estado"] == "ativo"]
        return {
            "servidores": lista,
            "coordenador": coord,
            "coordenador_caiu": bool(coord and servidores.get(coord, {}).get("estado") == "caiu"),
            "replicas_iguais": len(set(ativos)) <= 1,
            "diferenca": (max(ativos) - min(ativos)) if ativos else 0,
            "canais": sorted(canais.items()),
            "mensagens": list(mensagens),
            "eventos": list(eventos),
            "total": total,
            "uptime": int(agora - inicio),
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] == "/estado":
            corpo, tipo = json.dumps(estado()).encode(), "application/json"
        elif self.path.split("?")[0] == "/":
            with open(PAGINA, "rb") as f:
                corpo, tipo = f.read(), "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", tipo)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(corpo)

    def log_message(self, *args):      # sem log de cada requisição
        pass


if __name__ == "__main__":
    threading.Thread(target=ouvir, daemon=True).start()
    threading.Thread(target=vigiar, daemon=True).start()
    print(f"[painel] ouvindo {PROXY}; página em http://localhost:{PORTA}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORTA), Handler).serve_forever()
