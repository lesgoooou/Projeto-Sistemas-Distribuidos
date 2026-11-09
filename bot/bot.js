// Bot - Partes 1 e 2: login, canais, inscrição (SUB) e publicação (via servidor).
const zmq = require("zeromq");
const { encode, decode } = require("@msgpack/msgpack");

const NAME = process.env.BOT_NAME || "bot1";
const BROKER = process.env.BROKER_FRONTEND || "tcp://broker:5555";
const PROXY = process.env.PROXY_XPUB || "tcp://proxy:5558";   // onde o bot escuta

const FRASES = [
  "Olá, pessoal!", "Alguém por aí?", "Sistemas distribuídos são poggers",
  "ZeroMQ funcionando", "Mais uma mensagem aleatória", "Bom dia, canal!",
  "bruh!", "Qual é o rank de vocês?", "Ganka bot",
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hora = (ts) => new Date(ts * 1000).toISOString().slice(11, 23);
const agora = () => Date.now() / 1000;
const sorteia = (arr) => arr[Math.floor(Math.random() * arr.length)];

let clock = 0;   // relógio lógico (contador)

function log(direcao, msg) {
  console.log(
    `[${NAME}] ${direcao.padEnd(8)} tipo=${msg.tipo} ts=${hora(msg.ts)} clock=${msg.clock} ` +
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
  clock += 1;                                    // incrementa ANTES de enviar
  const msg = { tipo, ts: agora(), clock, dados };
  log("ENVIADO", msg);
  try {
    await req.send(Buffer.from(encode(msg)));
    const [raw] = await req.receive();
    const resp = decode(raw);
    clock = Math.max(clock, resp.clock || 0);      // ao receber: máximo(meu, recebido)
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

// Pede o histórico do canal (últimas 3 mensagens). Qualquer servidor responde com o histórico completo.
async function mostrarHistorico(canal) {
  const r = await enviar("historico", { canal, limite: 3 });
  if (r && r.status === "ok") {
    const ult = r.dados.mensagens.map((m) => `${m.usuario}: "${m.texto}"`);
    console.log(`[${NAME}] HISTORICO canal=${canal} total=${r.dados.total} ultimas=${JSON.stringify(ult)}`);
  }
}

// ---------------------------------------------------------------- socket SUB
const sub = new zmq.Subscriber();
sub.connect(PROXY);
const inscritos = new Set();   // única "memória" do bot: em quais canais ele está inscrito

function inscrever(canal) {
  sub.subscribe(canal);        // tópico = nome do canal
  inscritos.add(canal);
  console.log(`[${NAME}] INSCRITO  canal=${canal}`);
}

// Fica escutando o proxy o tempo todo, em paralelo ao laço principal
async function ouvir() {
  for await (const [topico, raw] of sub) {
    const canal = topico.toString();
    if (!inscritos.has(canal)) continue;   // a assinatura do ZMQ é por prefixo; filtra
    const m = decode(raw);
    clock = Math.max(clock, m.clock || 0);         // publicações também atualizam o relógio
    console.log(
      `[${NAME}] MENSAGEM  canal=${canal} de=${m.dados.usuario} clock=${m.clock} (meu=${clock}) ` +
        `texto="${m.dados.mensagem}" enviada=${hora(m.ts)} recebida=${hora(agora())}`
    );
  }
}

// --------------------------------------------------------------------- main
async function main() {
  novoSocket();
  ouvir();   // sem await: roda em paralelo

  // Login: se der erro (ou timeout), repete até funcionar
  while (true) {
    const r = await enviar("login", { usuario: NAME });
    if (r && r.status === "ok") break;
    console.log(`[${NAME}] login falhou, tentando de novo em 2s...`);
    await sleep(2000);
  }

  // Loop infinito. A cada volta:
  //   1) menos de 5 canais -> cria um novo
  //   2) inscrito em menos de 3 canais -> inscreve em mais um
  //   3) escolhe um canal e envia 10 mensagens (1 s de intervalo)
  // (1 e 2 ficam dentro do loop porque cada servidor conhece só parte dos canais:
  //  a lista completa só aparece depois de algumas voltas)
  while (true) {
    let canais = await listarCanais();

    if (canais.length < 5) {
      const nome = `canal_${Math.random().toString(36).slice(2, 6)}`;
      await enviar("criar_canal", { canal: nome });
      canais = await listarCanais();
    }

    const livres = canais.filter((c) => !inscritos.has(c));
    if (inscritos.size < 3 && livres.length > 0) {
      const novo = sorteia(livres);
      inscrever(novo);
      await mostrarHistorico(novo);   // Parte 5: ao assinar, recupera as mensagens anteriores do canal
    }

    if (canais.length === 0) { await sleep(1000); continue; }
    const canal = sorteia(canais);
    for (let i = 1; i <= 10; i++) {
      await enviar("publicar", { canal, usuario: NAME, mensagem: sorteia(FRASES) });
      await sleep(1000);
    }
  }
}

main();
