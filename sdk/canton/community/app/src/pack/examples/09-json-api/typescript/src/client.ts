import createClient from "openapi-fetch";
import type {paths} from "../generated/api/ledger-api";

export const client = createClient<paths>({baseUrl: "http://localhost:7575"});

export function valueOrError<T>(response: { data?: T, error?: any }): Promise<T> {
    if (response.data === undefined) {
        console.log(`Error ${JSON.stringify(response.error)}`)
        return Promise.reject(response.error);
    } else {
        return Promise.resolve(response.data);
    }
}
