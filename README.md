# Projeto BBS - CC7261 - Parte 4

## Como rodar
    docker compose up --build

## Linguagens
- Python: broker, proxy, serviço de referência e servidores
- Node.js (JavaScript): bots (clientes)

## Arquitetura
| Serviço | Função |
|---|---|
| broker | REQ/REP: ROUTER :5555 (bots) <-> DEALER :5556 (servidores), round-robin |
| proxy | PUB/SUB: XSUB :5557 (servidores publicam) <-> XPUB :5558 (bots e servidores assinam) |
| referencia | REP :5559, só servidores: rank, lista de servidores e heartbeat |
| server1/2 | atendem os bots, gravam em disco, publicam; elegem o coordenador e sincronizam o relógio |
| bot1/2 | login, criam canais, assinam canais e publicam mensagens |

## Mensagens
MessagePack. Envelope: `{tipo, ts, clock, dados}` (+ `status` e `msg` nas respostas).
Tipos entre servidores (porta 6000 de cada servidor): `relogio`, `eleicao`.
Anúncio do coordenador: publicação no tópico `servers` com `{coordenador: nome}`.
O nome `servers` é reservado: bots não podem criar nem publicar em um canal com esse nome.

## Relógio lógico
Contador por processo: incrementa antes de enviar; ao receber, assume o máximo entre o seu e o da mensagem.

## Eleição (rank mais alto vence)
- Ao entrar, e sempre que o coordenador some, o servidor pergunta (`eleicao`) aos servidores de rank maior.
- Quem responde OK assume a eleição. Se ninguém responde, o servidor se declara coordenador e publica o nome no tópico `servers`.
- Se um servidor receber o anúncio de um coordenador de rank menor que o seu, não o aceita e assume a eleição
  (resolve duas eleições simultâneas).
- O coordenador é considerado indisponível quando não responde ao pedido de relógio ou sai da lista da referência.
- O endereço de um servidor é `tcp://<nome>:6000`: o nome do servidor é o nome do serviço no docker-compose.

## Sincronização do relógio
A cada 15 mensagens de clientes recebidas, cada servidor pede a hora ao coordenador (`relogio`) e ajusta o relógio físico,
compensando metade do tempo de ida e volta. O coordenador não ajusta o próprio relógio.

## Referência
`rank`, `list` e `heartbeat` (a cada 10 mensagens de clientes). Quem fica 30 s sem heartbeat sai da lista.
A resposta do heartbeat não traz mais a hora.

## Persistência
SQLite por servidor (`./data/<server>/<server>.db`): `logins`, `canais` e `mensagens`.

## Limitação conhecida (resolvida na Parte 5)
Com round-robin, cada servidor conhece só parte dos canais e das mensagens.
