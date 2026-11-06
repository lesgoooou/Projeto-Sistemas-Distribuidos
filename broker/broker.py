"""Broker REQ/REP: bots (REQ) -> ROUTER :5555 -> DEALER :5556 -> servidores (REP).
O DEALER distribui as requisições em round-robin entre os servidores conectados."""
import zmq

ctx = zmq.Context()

frontend = ctx.socket(zmq.ROUTER)   # lado dos bots
frontend.bind("tcp://*:5555")

backend = ctx.socket(zmq.DEALER)    # lado dos servidores
backend.bind("tcp://*:5556")

print("[broker] ROUTER :5555  <->  DEALER :5556", flush=True)
zmq.proxy(frontend, backend)        # repassa mensagens nos dois sentidos
