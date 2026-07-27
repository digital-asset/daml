import createClient from "openapi-fetch";
import type {paths} from "../generated/api/ledger-api";
import {PARTICIPANT_HOST, PARTICIPANT_PORT} from "./environment";

const baseUrl = `http://${PARTICIPANT_HOST}:${PARTICIPANT_PORT}`;

export const client = createClient<paths>({baseUrl});

export function valueOrError<T>(response: { data?: T, error?: any }): Promise<T> {
    if (response.data === undefined) {
        console.log(`Error ${JSON.stringify(response.error)}`)
        return Promise.reject(response.error);
    } else {
        return Promise.resolve(response.data);
    }
}
