// para conectar à aplicação Cloud
const API_KEY = "#htxrlLaWaU3F8aNnjviFhreqyWzI1YowyZ8bFoCBNjhp8umKToLxTF4kau0tnp@";

// URL da Cloud
// const VPS_URL = "wss://saudenabr.algol.dev/loadtelemetry";
const VPS_URL = "ws://localhost:3003/loadtelemetry";

// Usaremos fetch nativo no lugar do @questdb/nodejs-client

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

    try {
        const payload = JSON.parse(event.data.toString()); // obtém os dados e os converte para JSON

        // maquina de estados para as mensagens
        switch (payload.action) {

            case "CONEXAO_ESTABELECIDA":
                console.log("🤝 Handshake aceito pela VPS. Solicitando o 1º lote de telemetria...");
                // uma vez estabelecida a conexão, pode-se fazer a primeira solicitação das telemetrias - FETCH
                solicitarLoteSeguro(500); // FETCH
                break;

            case "BATCH":
                // 🛑 MENSAGEM CHEGOU! Desarma o cão de guarda imediatamente
                if (watchdogFetch) clearTimeout(watchdogFetch);

                // determina o tamanho dos dados recebidos
                tamanhoUltimoLote = Array.isArray(payload.data) ? payload.data.length : 0;

                console.log(`📦 Lote recebido! Tamanho: ${tamanhoUltimoLote}`)
                console.log(payload.data);

                // Mapeia os dados recebidos para extrair as chaves que serão apagadas
                const keysToDelete = payload.data.map((item: any) => {
                    // Trata caso o item seja um array (CSV separado por vírgula)
                    const values = typeof item === 'string' ? item.split(',') : [item.unixTs, item.idMotorista];
                    return {
                        unixTs: values[0],
                        idMotorista: values[1]
                    };
                });

                // 2. Salvar no QuestDB
                console.log("💾 Salvando lote no QuestDB local...");

                try {
                    await salvarDadosQuestDB(payload.data);

                    // Avisamos a VPS que pode apagar do banco dela (só envia se o QuestDB salvou com sucesso)
                    console.log("🗑️ Enviando ACK para exclusão na VPS...");
                    ws.send(JSON.stringify({ action: "ACK", keys: keysToDelete }));
                } catch (err) {
                    console.error("Falha ao salvar no QuestDB, o ACK não será enviado para que não haja perda de dados.");
                }

                break;

            case "ACK_CONFIRMED":
                console.log("✅ VPS confirmou a exclusão do lote.");

                // se o ultimo lote veio lotado, o tempo de espera para a próxima requisição de dados é mínimo
                const tempoDeEspera = (tamanhoUltimoLote === 500) ? 100 : 5000;

                // após a confirmação de remoção da VPS, esperamos um tempo e enviamos nova requisição
                setTimeout(() => {
                    console.log(tempoDeEspera === 100 ? "⚡ Acelerando busca..." : "⏳ Aguardando novos dados...");
                    solicitarLoteSeguro(500); // FETCH
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

async function salvarDadosQuestDB(data: any[]) {
    try {
        // Itera sobre todos os dados do payload e monta o texto do QuestDB (InfluxDB Line Protocol)
        let linhasILP = "";
        
        for (const item of data) {
            let values = item.split(',');
            // Formato ILP: tabela,tag1=valor1,tag2=valor2 coluna1=valor1,coluna2=valor2 timestamp
            // O 'i' depois dos números indica que eles são inteiros no banco
            linhasILP += `telemetria,idMotorista=${values[1]} bpm=${values[2]}i,vfc=${values[3]}i,spo2=${values[4]}i ${values[0]}\n`;
        }

        // Usa o fetch nativo da Web/Bun para enviar as linhas (rápido e 100% compatível)
        const res = await fetch("http://127.0.0.1:9000/write?precision=ms", {
            method: "POST",
            body: linhasILP
        });

        if (!res.ok) {
            throw new Error(`QuestDB retornou erro HTTP ${res.status}: ${await res.text()}`);
        }
    } catch (error) {
        console.error("Erro ao salvar no QuestDB:", error);
        throw error; // Lança o erro para cima, impedindo o envio do ACK
    }
}
