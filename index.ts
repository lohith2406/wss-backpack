import WebSocket, { WebSocketServer } from "ws";
import { client } from "./redis";

async function poll() {
    while(true) {
        const response = await client.brPop("engine-outgoing", 0);
        if (!response) {
            continue;
        }
        // stream: "Trade.200ms.SOL_USDC", value:
        const parsedResponse = JSON.parse(response.element)
        const subscribers = activeSubscriptions[parsedResponse.stream];
        if (!subscribers) {
            continue
        }
        subscribers.forEach((socket) => socket.send(parsedResponse.value));
    }
} 

poll()

const wss = new WebSocketServer({
    port: 3000
});

const activeSubscriptions: Record<string, WebSocket[]> = {}

wss.on("connection", (socket) => {

    socket.on("message", (data) => {
        const parsedData = JSON.parse(data.toString());

        // {"method": "SUBSCRIBE", "params": ["trade.200ms.SOL_USDC"], "id": 1} 
        // {"method": "SUBSCRIBE", "params": ["depth.200ms.SOL_USDC"], "id": 1} 
        if (parsedData.method === "SUBSCRIBE") {
            parsedData.params.forEach((param: string) => {
            
                if (!activeSubscriptions[param]) {
                    activeSubscriptions[param] = [];
                }
                activeSubscriptions[param].push(socket);
                
            });
        }

        if (parsedData.method === "UNSUBSCRIBE") {
            parsedData.params.forEach((param: string) => {
                
                if (!activeSubscriptions[param]) {
                    return;
                }
                activeSubscriptions[param] = activeSubscriptions[param].filter((client) => client !== socket);
                
            });
        }
    })
})
