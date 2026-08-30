import WebSocket from "isomorphic-ws";
import {PARTICIPANT_HOST, PARTICIPANT_PORT} from "./environment";

const BASE_WS_URL = `ws://${PARTICIPANT_HOST}:${PARTICIPANT_PORT}`;

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

