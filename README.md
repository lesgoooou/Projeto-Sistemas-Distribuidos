# Projeto BBS - CC7261 - Parte 1

## Como rodar
    docker compose up --build

## Linguagens
- Python: broker e servidores
- Node.js (JavaScript): bots (clientes)

## Mensagens
Serializadas em MessagePack. Envelope: `{tipo, ts, dados}` (+ `status` e `msg` nas respostas).
`ts` = timestamp de envio (obrigatório). Tipos: `login`, `criar_canal`, `listar`, `resposta`.

## Erros tratados
- login: nome vazio/inválido -> o bot repete o login
- criar canal: nome inválido ou canal já existente -> `status=erro`

## Persistência
SQLite por servidor (`./data/<server>/<server>.db`):
- `logins(usuario, ts)` - todos os logins com timestamp
- `canais(nome)` - canais criados
