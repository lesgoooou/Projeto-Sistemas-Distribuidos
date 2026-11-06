// Bot - Parte 1: login, listar canais e criar canal.
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const NAME = process.env.BOT_NAME || "bot1";
const BROKER = process.env.BROKER_FRONTEND || "tcp://broker:5555";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hora = (ts) => new Date(ts * 1000).toISOString().slice(11, 23);
const agora = () => Date.now() / 1000;

function log(direcao, msg) {
  console.log(
    `[${NAME}] ${direcao.padEnd(8)} tipo=${msg.tipo} ts=${hora(msg.ts)} ` +
      `dados=${JSON.stringify(msg.dados)}` +
      (msg.status ? ` status=${msg.status} ${msg.msg}` : "")
  );
}

// ---------------------------------------------------------------- socket REQ
let req = null;
function novoSocket() {
  if (req) req.close();
  req = new zmq.Request({ receiveTimeout: 5000, sendTimeout: 5000 });
  req.connect(BROKER);
}

// Envia uma requisição e devolve a resposta (ou null se der timeout).
// REQ/REP é estrito (send -> receive): se falhar, o socket precisa ser recriado.
async function enviar(tipo, dados = {}) {
  const msg = { tipo, ts: agora(), dados };
  log("ENVIADO", msg);
  try {
    await req.send(Buffer.from(encode(msg)));      // serializa em MessagePack
    const [raw] = await req.receive();
    const resp = decode(raw);
    log("RECEBIDO", resp);
    return resp;
  } catch (e) {
    console.log(`[${NAME}] ERRO     ${tipo}: ${e.message} (recriando socket)`);
    novoSocket();
    return null;
  }
}

async function listarCanais() {
  const r = await enviar("listar");
  return r && r.status === "ok" ? r.dados.canais : [];
}

// --------------------------------------------------------------------- main
async function main() {
  novoSocket();

  // 1) Login: se der erro (ou timeout), repete até funcionar
  while (true) {
    const r = await enviar("login", { usuario: NAME });
    if (r && r.status === "ok") break;
    console.log(`[${NAME}] login falhou, tentando de novo em 2s...`);
    await sleep(2000);
  }

  // 2) Lista os canais que já existem
  let canais = await listarCanais();
  console.log(`[${NAME}] canais existentes: ${JSON.stringify(canais)}`);

  // 3) Critério (escolha do grupo): cria um canal novo se existirem menos de 5
  if (canais.length < 5) {
    const nome = `canal_${NAME}_${Math.random().toString(36).slice(2, 6)}`;
    const r = await enviar("criar_canal", { canal: nome });
    if (r && r.status === "erro") console.log(`[${NAME}] não criou o canal: ${r.msg}`);
  }

  // 4) Lista 2 vezes. Como o broker alterna entre os servidores (round-robin) e cada
  //    servidor tem só os seus dados, a lista pode mudar de uma consulta para outra.
  for (let i = 1; i <= 2; i++) {
    canais = await listarCanais();
    console.log(`[${NAME}] consulta ${i}/2 -> canais: ${JSON.stringify(canais)}`);
  }
  process.exit(0);
}

main();
