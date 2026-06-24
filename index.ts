import { conectarVPS } from './src/getTelemetry'

// para conectar à aplicação Cloud
const API_KEY = "#htxrlLaWaU3F8aNnjviFhreqyWzI1YowyZ8bFoCBNjhp8umKToLxTF4kau0tnp@";

// URL da Cloud
const VPS_URL = "wss://saudenabr.algol.dev/loadtelemetry";
// const VPS_URL = "ws://localhost:3003/loadtelemetry";

console.log("🚀 Iniciando o Servidor...");

conectarVPS(VPS_URL, API_KEY);
