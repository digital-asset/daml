import WebSocket from "isomorphic-ws";

const BASE_WS_URL = 'ws://localhost:7575';

export async function connectWs(uri: string): Promise<WebSocket> {
    return new Promise<WebSocket>((resolve, reject) => {
        const ws = new WebSocket(BASE_WS_URL + uri, ["daml.ws.auth"]);
        ws.onopen = () => {
            resolve(ws);
        };
        ws.onerror = (err: any) => {
            reject(err);
        };
    })

}

