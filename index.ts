// ====================================================================
// CLIENTE LABORATÓRIO (APP 1) - INGESTÃO DE DADOS
// ====================================================================

// para conectar na VPS
const API_KEY = "#htxrlLaWaU3F8aNnjviFhreqyWzI1YowyZ8bFoCBNjhp8umKToLxTF4kau0tnp@";

// URL da VPS
// const VPS_URL = "wss://saudenabr.algol.dev/loadtelemetry";
const VPS_URL = "ws://localhost:3003/loadtelemetry";

console.log("🚀 Iniciando o Servidor...");

// O Bun usa WebSocket nativo da Web API
const ws = new WebSocket(VPS_URL, {
    headers: {
        "authorization": API_KEY
    }
});

// para controlar o delay das próximas requisições
let tamanhoUltimoLote = 0;

// timer para reenvio de solicitações por falhas (padrão Watchdog)
let watchdogFetch: Timer | null = null;

// ====================================================================
// FUNÇÕES DO WEBSOCKET
// ====================================================================

// 1. Evento de Abertura TCP
ws.onopen = () => {
    console.log("🌐 Conexão TCP aberta com a VPS. Aguardando Handshake...");
};

// 2. Evento Principal: Ouvindo as mensagens da VPS
ws.onmessage = async (event) => {

    const payload = event.data.toString(); // obtém os dados e os converte para string

    try {
        const data = JSON.parse(payload); // faz o parse do payload

        // maquina de estados para as mensagens
        switch (data.action) {

            case "CONEXAO_ESTABELECIDA":
                console.log("🤝 Handshake aceito pela VPS. Solicitando o 1º lote de telemetria...");
                // uma vez estabelecida a conexão, pode-se fazer a primeira solicitação das telemetrias - FETCH
                solicitarLoteSeguro(500);
                break;

            case "BATCH":
                // 🛑 MENSAGEM CHEGOU! Desarma o cão de guarda imediatamente
                if (watchdogFetch) clearTimeout(watchdogFetch);

                // determina o tamanho dos dados recebidos
                tamanhoUltimoLote = Array.isArray(data.data) ? data.data.length : 0;

                console.log(`📦 Lote recebido! Tamanho: ${tamanhoUltimoLote}`)

                // TODO: No futuro, é exatamente AQUI que vamos:
                // 1. Passar os dados no ONNX (IA)
                // 2. Salvar no DuckDB

                console.log("⚙️ Simulando processamento e inferência...");
                // Vamos simular que processamos e extraímos as chaves para deletar
                const fakeKeys = [{ unixTs: 1747238400, idMotorista: "001" }];

                // Avisamos a VPS que pode apagar do banco dela
                console.log("🗑️ Enviando ACK para exclusão na VPS...");
                ws.send(JSON.stringify({ action: "ACK", keys: fakeKeys }));

                break;

            case "ACK_CONFIRMED":
                console.log("✅ VPS confirmou a exclusão do lote.");

                // se o ultimo lote veio lotado, o tempo de espera para a próxima requisição de dados é mínimo
                const tempoDeEspera = (tamanhoUltimoLote === 500) ? 100 : 5000;

                // após a confirmação de remoção da VPS, esperamos um tempo e enviamos nova requisição
                setTimeout(() => {
                    console.log(tempoDeEspera === 100 ? "⚡ Acelerando busca..." : "⏳ Aguardando novos dados...");
                    solicitarLoteSeguro(500);
                }, tempoDeEspera);
                break;

            case "ERROR":
                console.error("❌ VPS reportou erro:", data.message);
                break;

            default:
                console.log("❓ Ação desconhecida recebida:", data);
        }

    } catch (err) {
        console.error("❌ Erro ao processar mensagem da VPS:", err);
    }
}

// 3. Evento de Fechamento
ws.onclose = () => {
    console.log("🔌 Conexão encerrada com a VPS.");
    // TODO: No futuro, colocaremos uma rotina aqui para tentar reconectar automaticamente
};

// 4. Evento de Erro de Rede
ws.onerror = (error) => {
    console.error("💥 Erro no protocolo WebSocket:", error);
};


// ====================================================================
// FUNÇÕES INTERNAS
// ====================================================================

// Função centralizada para pedir dados com segurança
function solicitarLoteSeguro(limit = 500) {
    console.log("🔄 Enviando FETCH para a VPS...");
    ws.send(JSON.stringify({ action: "FETCH", limit }));

    // Se já tinha um cão de guarda rodando, cancelamos ele
    if (watchdogFetch) clearTimeout(watchdogFetch);

    // Armamos um novo cronômetro de 10 segundos
    watchdogFetch = setTimeout(() => {
        console.warn("⚠️ Watchdog: A VPS não respondeu ao FETCH em 10s. Forçando reenvio...");
        solicitarLoteSeguro(limit); // Tenta de novo!
    }, 10000);
}
