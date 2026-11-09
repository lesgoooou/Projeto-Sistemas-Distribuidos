# Projeto BBS - CC7261 - Parte 5 (versão final)

Sistema de mensagens instantâneas no estilo BBS/IRC, feito só com bots. Usa ZeroMQ, MessagePack e Docker Compose.

## Como rodar
    docker compose up --build

## Linguagens
- Python: broker, proxy, serviço de referência e servidores
- Node.js (JavaScript): bots (clientes)

## Arquitetura
| Serviço | Função |
|---|---|
| broker | REQ/REP: ROUTER :5555 (bots) <-> DEALER :5556 (servidores), balanceamento round-robin |
| proxy | PUB/SUB: XSUB :5557 (servidores publicam) <-> XPUB :5558 (bots e servidores assinam) |
| referencia | REP :5559, só servidores: rank, lista de servidores e heartbeat |
| server1/2 | atendem os bots, gravam em disco, publicam, replicam dados, elegem o coordenador e sincronizam o relógio |
| bot1/2 | login, criam canais, assinam canais, pedem o histórico e publicam mensagens |

## Mensagens
MessagePack. Envelope: `{tipo, ts, clock, dados}` (+ `status` e `msg` nas respostas).
- Bot <-> servidor (via broker): `login`, `criar_canal`, `listar`, `publicar`, `historico`, `resposta`.
- Servidor -> proxy -> bots: publicação no tópico com o nome do canal.
- Servidor <-> referência: `rank`, `list`, `heartbeat`.
- Servidor <-> servidor (porta 6000 de cada servidor): `eleicao`, `relogio`, `sincronizar`.
- Servidor -> proxy -> servidores: tópico `servers` (anúncio do coordenador) e tópico `replica` (registros replicados).
- Os nomes `servers` e `replica` são reservados: bots não podem criar nem publicar em canais com esses nomes.

## Relógio lógico
Contador por processo: incrementa antes de enviar; ao receber, assume o máximo entre o seu e o da mensagem.
Todo registro gravado guarda o valor do contador de quando foi criado, e o histórico é ordenado por ele.

## Eleição e sincronização do relógio
- Eleição pelo rank (o maior vence): ao entrar e sempre que o coordenador some, o servidor pergunta (`eleicao`) aos servidores
  de rank maior; se nenhum responde, ele se declara coordenador e publica o nome no tópico `servers`.
- A cada 15 mensagens de clientes, cada servidor pede a hora ao coordenador (`relogio`) e ajusta o relógio físico.
- O endereço de um servidor é `tcp://<nome>:6000` (o nome do servidor é o nome do serviço no docker-compose).

## Serviço de referência
`rank`, `list` e `heartbeat` (a cada 10 mensagens de clientes). Quem fica 30 s sem heartbeat sai da lista.

## Persistência
SQLite por servidor (`./data/<server>/<server>.db`), sem compartilhar arquivo. Toda linha tem `id` único, `origem`
(servidor que a criou) e `clock` (relógio lógico de quando foi criada):
- `logins(id, origem, clock, usuario, ts)`
- `canais(id, origem, clock, nome, ts)`
- `mensagens(id, origem, clock, canal, usuario, texto, ts_envio, ts_publicacao)`

Bancos de execuções anteriores (Partes 1 a 4) não tinham `id`: ao subir, o servidor renomeia as tabelas antigas para
`*_antigo` (os dados antigos ficam guardados, mas não são replicados).

## Replicação e consistência

### O problema
O broker distribui os pedidos dos bots entre os servidores em round-robin, então cada servidor guardava só uma parte
dos logins, canais e mensagens. Se um servidor parasse, essa parte do histórico se perdia, e quem pedisse a lista de canais
ou o histórico recebia respostas diferentes dependendo de qual servidor atendia. O requisito é que todos os servidores
tenham todos os dados.

### Método escolhido
**Replicação com escrita em qualquer réplica (multi-mestre), com consistência eventual.** Cada servidor continua aceitando
escritas dos bots (o broker já faz esse balanceamento) e propaga cada registro novo aos outros. Um mecanismo de
**anti-entropia** (comparação de conteúdo entre servidores) garante que quem perdeu registros acabe recuperando.

Por que este método: os dados do sistema só crescem (login, canal criado e mensagem nunca são alterados nem removidos),
então não existem conflitos de escrita: juntar os dados de dois servidores é fazer a união dos registros. Isso permite
convergir sem um servidor primário e sem bloquear o atendimento dos bots para esperar confirmações.

### Como foi implementado
1. **Registros com identificador único.** Toda escrita vira um registro com `id` único (nome do servidor + sufixo
   aleatório). Para canais o `id` é derivado do nome (`canal:<nome>`), de modo que dois servidores que criem o mesmo
   canal ao mesmo tempo produzem o mesmo registro.
2. **Propagação.** Depois de gravar um registro novo, o servidor o publica no tópico `replica` do proxy (Pub/Sub).
   Todos os servidores assinam esse tópico e gravam o que recebem.
3. **Gravação idempotente.** A gravação usa `INSERT OR IGNORE` pelo `id`: receber o mesmo registro duas vezes (ou o próprio
   eco) não duplica nada. Um servidor que recebe uma réplica não a republica, e não a publica de novo no canal, para
   evitar laços e mensagens duplicadas para os bots.
4. **Recuperação ao voltar.** Antes de atender bots, um servidor que entra (ou volta de uma queda) manda a um servidor
   vivo a lista de `id`s que já tem (`sincronizar`) e grava os registros que faltam, todos os que foram criados
   enquanto esteve fora.
5. **Conferência periódica.** A cada 100 mensagens de clientes, o servidor repete essa comparação com outro servidor
   escolhido ao acaso, corrigindo qualquer publicação perdida no caminho (o Pub/Sub não garante entrega).
6. **Ordem.** O histórico de um canal (`historico`) é ordenado por `clock` lógico, depois por servidor de origem, então
   todos os servidores devolvem a mesma ordem.
7. **Observabilidade.** A cada heartbeat o servidor imprime `DADOS logins=.. canais=.. mensagens=..`; ao final, os números
   iguais nos servidores mostram que a replicação convergiu. Os bots, ao assinar um canal, pedem o histórico dele.

### Mudanças feitas no método para funcionar no projeto
- **Sem primário:** a replicação clássica com primário e cópias exigiria encaminhar todas as escritas ao primário; aqui
  o broker já espalha as escritas, então qualquer servidor aceita escrever e o conflito é evitado pela natureza dos
  dados (só inclusões).
- **Propagação por Pub/Sub** (tópico `replica`) em vez de conexões ponto a ponto, para não bloquear o servidor esperando
  respostas (os servidores são de uma só thread) e reaproveitar o proxy já existente. Em troca, a entrega não é garantida,
  e por isso foram adicionadas a recuperação ao voltar e a conferência periódica.
- **Identificadores e gravação idempotente**, necessários porque o Pub/Sub pode entregar mais de uma vez ou perder.
- **Canal criado implicitamente:** se uma publicação chegar a um servidor antes da réplica do canal (milissegundos), o
  servidor registra o canal (e o replica) em vez de rejeitar a mensagem.

### Garantias e limites
- Consistência **eventual**: logo depois de uma escrita, outro servidor pode ainda não ter o registro (milissegundos).
- Se um servidor cair, nenhum dado se perde (cada um tem cópia de tudo) e, ao voltar, ele recupera o que perdeu.
- Se todos os servidores caírem ao mesmo tempo, cada banco continua em disco e as diferenças são reconciliadas na volta.
- Na comparação, o servidor envia a lista de todos os `id`s que tem: serve para o porte do projeto, mas para um volume
  grande seria melhor trocar só um resumo (por exemplo, o maior número de sequência de cada servidor de origem).

## Limitações conhecidas da sincronização de relógio
- **O relógio do coordenador é, por definição, o correto.** Não existe fonte externa de hora (a referência deixou de
  fornecê-la na Parte 4), então se o coordenador estiver atrasado ou adiantado, todos os outros copiam o erro. O grupo fica
  consistente entre si (o que importa para ordenar registros), mas pode ficar deslocado da hora real.
- **Troca de coordenador:** o novo coordenador mantém o relógio que já tinha (o ajuste da sua última sincronização).
- **Sem média:** foi implementada a troca `relógio -> hora correta` com o coordenador, como no enunciado. O algoritmo de
  Berkeley completo calcula a média dos relógios, o que diluiria o erro de um relógio ruim.
- No Docker todos os containers usam o relógio da mesma máquina; a divergência só aparece se simulada (`CLOCK_SKEW`).

## Outras observações
- O heartbeat depende de mensagens de clientes: sem bots, os servidores deixam de avisar e saem da lista da referência
  após o tempo limite.
