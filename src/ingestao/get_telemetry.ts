import { salvarDadosQuestDB, salvarRedisStream } from '../../db/telemetria.DAO';


// para conectar à aplicação Cloud
const API_KEY = "#htxrlLaWaU3F8aNnjviFhreqyWzI1YowyZ8bFoCBNjhp8umKToLxTF4kau0tnp@";

// URL da Cloud
const VPS_URL = "wss://saudenabr.algol.dev/loadtelemetry";
// const VPS_URL = "ws://localhost:3003/loadtelemetry";

// O Bun usa WebSocket nativo da Web API
export let ws: WebSocket | null = null;

// para controlar o delay das próximas requisições
export let tamanhoUltimoLote = 0;

// timer para reenvio de solicitações por falhas (padrão Watchdog)
export let watchdogFetch: Timer | null = null;

let currentVpsUrl = "";
let currentApiKey = "";
const tamanhoMaxLote = 5000;

export function conectarVPS(vpsUrl?: string, apiKey?: string) {
    if (vpsUrl) currentVpsUrl = vpsUrl;
    if (apiKey) currentApiKey = apiKey;

    console.log(`🔄 Tentando conectar com a VPS...`);
    ws = new WebSocket(currentVpsUrl, {
        headers: {
            "authorization": currentApiKey
        }
    });

    // 1. Evento de Abertura TCP
    ws.onopen = onOpen;

    // 2. Evento Principal: Ouvindo as mensagens da VPS
    ws.onmessage = onMessage;

    // 3. Evento de Fechamento
    ws.onclose = onClose;

    // 4. Evento de Erro de Rede
    ws.onerror = onError;
}

// ====================================================================
// FUNÇÕES DO CICLO DE VIDA O WEBSOCKET
// ====================================================================

// 1. Evento de Abertura TCP
export function onOpen() {
    console.log("🌐 Conexão TCP aberta com a VPS. Aguardando Handshake...");
};

// 2. Evento Principal: Ouvindo as mensagens da VPS
export async function onMessage(event: any) {

    try {
        const payload = JSON.parse(event.data.toString()); // obtém os dados e os converte para JSON

        // maquina de estados para as mensagens
        switch (payload.action) {

            case "CONEXAO_ESTABELECIDA":
                console.log("🤝 Handshake aceito pela VPS. Solicitando o 1º lote de telemetria...");
                // uma vez estabelecida a conexão, pode-se fazer a primeira solicitação das telemetrias - FETCH
                solicitarLoteSeguro(tamanhoMaxLote); // FETCH
                break;

            case "BATCH":
                // 🛑 MENSAGEM CHEGOU! Desarma o cão de guarda imediatamente
                if (watchdogFetch) clearTimeout(watchdogFetch);

                // determina o tamanho dos dados recebidos
                tamanhoUltimoLote = Array.isArray(payload.data) ? payload.data.length : 0;

                if (tamanhoUltimoLote == 0) {
                    console.log(`📦 Lote recebido está Vazio!`);
                    setTimeout(() => {
                        solicitarLoteSeguro(tamanhoMaxLote); // FETCH
                    }, 500);
                    return; // Interrompe a execução aqui (não salva no banco nem manda ACK)
                }

                console.log(`📦 Lote recebido! Tamanho: ${tamanhoUltimoLote}`)
                //console.log(payload.data);

                // Mapeia os dados recebidos para extrair as chaves que serão enviadas para remoção no CLOUD
                const keysToDelete = payload.data.map((item: any) => {
                    // Trata caso o item seja um array (CSV separado por vírgula)
                    const values = typeof item === 'string' ? item.split(',') : [item.unixTs, item.idMotorista];
                    return {
                        unixTs: values[0],
                        idMotorista: values[1]
                    };
                });

                // 2. Salvar nas estruturas de real time (janelas deslizantes)
                console.log("🚀 Enviando lote para a Esteira (Stream) do Redis...");
                try {
                    await salvarRedisStream(payload.data);
                } catch (err) {
                    console.error("Falha ao scolocar dados na esteira do Redis. O ACK não será enviado para que não haja perda de dados.");
                }

                // 3. Salvar no QuestDB
                console.log("💾 Salvando lote no QuestDB local...");
                try {
                    await salvarDadosQuestDB(payload.data);

                    // Avisamos a VPS que pode apagar do banco dela (só envia se o QuestDB salvou com sucesso)
                    console.log("🗑️ Enviando ACK para exclusão na VPS...");
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ action: "ACK", keys: keysToDelete }));
                    }
                } catch (err) {
                    console.error("Falha ao salvar no QuestDB. O ACK não será enviado para que não haja perda de dados.");
                }


                break;

            case "ACK_CONFIRMED":
                console.log("✅ VPS confirmou a exclusão do lote.");

                // se o ultimo lote veio lotado, o tempo de espera para a próxima requisição de dados é mínimo
                const tempoDeEspera = (tamanhoUltimoLote === tamanhoMaxLote) ? 100 : 500;

                // após a confirmação de remoção da VPS, esperamos um tempo e enviamos nova requisição
                setTimeout(() => {
                    console.log(tempoDeEspera === 100 ? "⚡ Acelerando busca..." : "⏳ Aguardando novos dados...");
                    solicitarLoteSeguro(tamanhoMaxLote); // FETCH
                }, tempoDeEspera);
                break;

            case "ERROR":
                console.error("❌ VPS reportou erro:", payload.message);
                break;

            default:
                console.log("❓ Ação desconhecida recebida:", payload);
        }

    } catch (err) {
        console.error("❌ Erro ao processar mensagem da VPS:", err);
    }
}

// 3. Evento de Fechamento
export function onClose() {
    console.log("🔌 Conexão encerrada com a VPS. Tentando reconectar em 5 segundos...");
    if (watchdogFetch) {
        clearTimeout(watchdogFetch);
        watchdogFetch = null;
    }
    setTimeout(conectarVPS, 5000);
};

// 4. Evento de Erro de Rede
export function onError(error: any) {
    console.error("💥 Erro no protocolo WebSocket. A conexão será fechada.");
    ws?.close();
};

// ====================================================================
// FUNÇÕES INTERNAS
// ====================================================================

// Função centralizada para pedir dados com segurança
function solicitarLoteSeguro(limit = 500) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        console.log("⏳ WebSocket não está aberto. Aguardando reconexão...");
        return;
    }

    console.log("\n🔄 Enviando FETCH para a VPS...");
    ws.send(JSON.stringify({ action: "FETCH", limit }));

    // Se já tinha um cão de guarda rodando, cancelamos ele
    if (watchdogFetch) clearTimeout(watchdogFetch);

    // Armamos um novo cronômetro de 10 segundos
    watchdogFetch = setTimeout(() => {
        console.warn("⚠️ Watchdog: A VPS não respondeu ao FETCH em 10s. Forçando reenvio...");
        solicitarLoteSeguro(limit); // Tenta de novo!
    }, 10000);
}

console.log("🚀 Iniciando o Servidor...");
conectarVPS(VPS_URL, API_KEY);