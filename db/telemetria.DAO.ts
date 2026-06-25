import Redis from "ioredis";

// Cache em memória para guardar as chaves já processadas recentemente
// Isso evita que, se a VPS reenviar o mesmo lote (porque o ACK falhou),
// ou se houver linhas duplicadas no mesmo lote, elas não sejam inseridas no QuestDB.
const cacheChavesProcessadas = new Set<string>();

const redis = new Redis({
    host: "127.0.0.1",
    port: 6379
})

export async function salvarDadosQuestDB(data: any[]) {
    try {
        let linhasILP = "";
        let qtdeNovos = 0;

        for (const item of data) {
            let values = item.split(',');
            // uniqueKey combinando timestamp (values[0]) e idMotorista (values[1])
            const uniqueKey = `${values[0]}-${values[1]}`;

            // Se já processamos essa chave recentemente, pula (evita duplicidade)
            if (cacheChavesProcessadas.has(uniqueKey)) {
                continue;
            }

            cacheChavesProcessadas.add(uniqueKey);
            qtdeNovos++;

            // Formato ILP: tabela,tag1=valor1,tag2=valor2 coluna1=valor1,coluna2=valor2 timestamp
            // O 'i' depois dos números indica que eles são inteiros no banco
            linhasILP += `telemetria,idMotorista=${values[1]} bpm=${values[2]}i,vfc=${values[3]}i,spo2=${values[4]}i ${values[0]}\n`;
        }

        // Previne vazamento de memória: limpa o cache se ficar muito grande (ex: guarda os últimos 100.000)
        // Como o tamanho do lote máximo é 5000, 100.000 é mais que suficiente para garantir 
        // que lotes antigos já receberam ACK e não serão reenviados.
        if (cacheChavesProcessadas.size > 100000) {
            cacheChavesProcessadas.clear();
        }

        // Se todos os dados eram duplicados e não sobrou nada novo para inserir, podemos abortar
        if (qtdeNovos === 0) {
            return;
        }

        // Usa o fetch nativo da Web/Bun para enviar as linhas (rápido e 100% compatível)
        const res = await fetch("http://127.0.0.1:9000/write?precision=s", {
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

export async function salvarRedisStream(data: any[]) {
    try {
        // pipeline para envio multiplo
        const pipeline = redis.pipeline();
        for (const item of data) {
            let values = typeof item === 'string' ? item.split(',') : [item.unixTs, item.idMotorista, item.bpm, item.vfc, item.spo2];

            const timestamp = values[0];
            const idMotorista = values[1];
            const bpm = values[2];
            const vfc = values[3];
            const spo2 = values[4];

            // XADD: Adiciona na stream dedicada do motorista
            pipeline.xadd(
                `telemetria:stream:${idMotorista}`, // Uma esteira dedicada por motorista
                'MAXLEN', '~', 5000, // Mantém até 5.000 itens (dá uma folga confortável além dos 3600 itens de 1h)
                '*', // O '*': Pede para o Redis criar um ID único automático para essa mensagem
                'timestamp', timestamp,
                'bpm', bpm,
                'vfc', vfc,
                'spo2', spo2
            );
        }
        // Executa todo o lote no Redis de uma vez
        await pipeline.exec();

    } catch (error) {
        console.error("Erro ao salvar no Redis Stream:", error);
        throw error; // Lança para o getTelemetry lidar
    }
}

