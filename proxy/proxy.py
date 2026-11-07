"""Proxy Pub/Sub: servidores (PUB) -> XSUB :5557 -> XPUB :5558 -> bots (SUB)."""
import zmq

ctx = zmq.Context()

xsub = ctx.socket(zmq.XSUB)      # lado de quem publica (servidores)
xsub.bind("tcp://*:5557")

xpub = ctx.socket(zmq.XPUB)      # lado de quem assina (bots)
xpub.bind("tcp://*:5558")

print("[proxy] XSUB :5557  <->  XPUB :5558", flush=True)
zmq.proxy(xsub, xpub)            # repassa publicações e também as inscrições
