import Redis from "ioredis";

// ====================================================================
// FORMATOS
// ====================================================================

interface LeituraFormatada {
  timestamp: string;
  bpm: string;
  vfc: string;
  sop2: string;
}

// ====================================================================
// DADOS
// ====================================================================

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379
});

const stramsPrefixo = "telemetria:stream:*"; // prefixo dos streams
const tempoEsperaStream = 1000;

// Dicionário para armazenar o último ID lido de cada stream (por motorista)
const ultimosIdsLidos: Record<string, string> = {};

// um hashmap para armazenar as janelas de cada motorista.
// cada registro (janela) possui o id do motorista (chave) e uma lista de leituras (valor)
export const janelasDosMotoristas: Record<string, LeituraFormatada[]> = {};

// ====================================================================
// FUNÇÕES
// ====================================================================

console.log("📊 Servidor do Dashboard (Análise em Tempo Real) Iniciado!");



async function escutarStreams() {
  console.log("Aguardando novos stream de dados...");

  // loop continuo das leituras dos streams
  while (true) {

    const chaves = await obertChavesStreamsRedis(); // 1. Descobre a qtd de streams (uma por motorista) que existem no Redis
    if (chaves.length === 0) { // se não houver streams
      console.log("Sem dados! Aguardando novos stream de dados...");
      await new Promise(r => setTimeout(r, tempoEsperaStream)); // espera um pouco
      continue; // encerra esse loop aqui
    }

    // Obtem uma lista de streams, onde cada stream representa as leituras de um motorista
    const streams = await obterStreamsRedis(chaves, ultimosIdsLidos);

    if (!streams) continue; // Se esgotar o tempo do BLOCK sem novas mensagens (ou houver erro), o Redis retorna null. Ignoramos e tentamos de novo.

    // percorre o stream de cada motorista
    for (const stream of streams) {

      // Remove o prefixo para usar apenas o ID (ex: "123") como chave do dicionário
      const idMotorista = stream[0].replace("telemetria:stream:", "");
      const streamFormatado = formatarStream(stream); // obtém o stream formatado em uma lista de obj json

      trataJanelas(idMotorista, streamFormatado);
      console.log(idMotorista);
    }
    console.log(`---------------------------------`);
  }
}

/** Retorna a quantidade de streams */
async function obertChavesStreamsRedis(): Promise<string[]> {
  return await redis.keys(stramsPrefixo); // o id da fila
}

/** Retorna os streams do redis */
async function obterStreamsRedis(chaves: string[], ultimosIdsLidos: Record<string, string>) {
  // Para cada chave, pegamos o último ID lido. 
  // IMPORTANTE: Ao descobrir uma stream nova, devemos usar '0-0' para ler desde o começo.
  // Se usarmos '$', ignoraremos as primeiras mensagens (aquelas que criaram a stream!), causando demora.
  const ids = chaves.map(chave => ultimosIdsLidos[chave] || '0-0');

  try {
    // XREAD lê múltiplas streams. BLOCK paralisa a execução até que mensagens cheguem ou o tempo esgote.
    const streams = await redis.xread(
      "BLOCK",
      tempoEsperaStream,
      "STREAMS",
      ...chaves,
      ...ids
    ) as any[];

    return streams;
  } catch (erro) {
    console.error("Erro ao executar XREAD no Redis:", erro);
    return null;
  }
}

/** Transforma cada stream em um array de objetos json bem formatados */
function formatarStream(stream: any): LeituraFormatada[] {
  const streamName = stream[0]; // obtém o nome (id do motorista)
  const streamData = stream[1]; // obtém a lista de dados do stream
  const resultados: LeituraFormatada[] = [];

  // percorre cada um dos dados do stream
  for (const leitura of streamData) {
    const id = leitura[0];
    const dados = leitura[1];
    ultimosIdsLidos[streamName] = id;

    resultados.push({
      "timestamp": dados[1],
      "bpm": dados[3],
      "vfc": dados[5],
      "sop2": dados[7] // lembrando que no producer (DAO) está spo2, mas na sua interface LeituraFormatada está sop2
    });
  }

  return resultados;
}

/** Faz as inseções nas janelas deslizantes de cada motorista */
function trataJanelas(idMotorista: string, streamFormatado: LeituraFormatada[]) {

  // Se esse motorista ainda não tem uma janela, criamos uma nova pra ele (array vazio)
  if (!janelasDosMotoristas[idMotorista]) {
    janelasDosMotoristas[idMotorista] = [];
    console.log(`🆕 Nova janela criada para o motorista: ${idMotorista}`);
  }

  // Adicionamos as novas leituras à janela já existente desse motorista
  for (const leitura of streamFormatado) {
    janelasDosMotoristas[idMotorista].push(leitura);
    // verifica o limite da janela
    if (janelasDosMotoristas[idMotorista].length > 15)
      janelasDosMotoristas[idMotorista].shift(); // remove o mais antigo
  }

  const infos = janelasDosMotoristas[idMotorista];

  server.publish("dashboard", JSON.stringify({
    tipo: "NOVAS_LEITURAS",
    idMotorista: idMotorista,
    novosDados: streamFormatado,
    janelaAtualizada: janelasDosMotoristas[idMotorista] // opcional: mandar como ficou a janela após os pushs
  }));
}

escutarStreams();

// ====================================================================
// SEVIDOR WEBSOCKET PARA ENVIO DOS DADOS PARA O FRONTEND
// ====================================================================

// Adicionado no final do server.ts
const server = Bun.serve({
  port: 3000,
  fetch(req, server) {
    if (server.upgrade(req)) return; // Transforma requisição HTTP em WebSocket
    return new Response("Servidor do Dashboard Ativo");
  },
  websocket: {
    open(ws) {
      ws.subscribe("dashboard"); // Avisa que esse cliente quer ouvir o canal 'dashboard'
      // Manda o estado atual completo pro frontend assim que ele abre a tela!
      ws.send(JSON.stringify({ tipo: "ESTADO_INICIAL", dados: janelasDosMotoristas }));
    },
    message(ws, message) { },
  },
});