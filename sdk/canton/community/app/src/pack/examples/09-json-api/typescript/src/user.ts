import {client, valueOrError} from "./client";
import type {components} from "../generated/api/ledger-api";

/*
This utility contains example ledger_api functions for allocating use and party
 */
export async function getUserParty(user: string): Promise<string | undefined> {
    const {data, error} = await client.GET("/v2/users/{user-id}", {
        params: {
            path: {"user-id": user},
        },
    });
    if (data !== undefined) {
        return Promise.resolve(data.user?.primaryParty);
    } else {
        //we assume USER_NOT_FOUND
        return Promise.resolve(undefined);
    }
}

export async function allocateParty(user: string): Promise<string | undefined> {

    const namespace = await getParticipantNamespace();
    const party = await getParty(`${user}::${namespace}`);

    if (party !== undefined) {
        Promise.resolve(party.party);
    }
    const resp = await client.POST("/v2/parties", {
        body: {
            partyIdHint: user,
            identityProviderId: "",
        }
    });
    return valueOrError(resp).then(data => data.partyDetails?.party);
}

export async function getParty(user: string): Promise<components["schemas"]["PartyDetails"] | undefined> {
    const {data, error} = await client.GET("/v2/parties/{party}", {
        params: {
            path: {"party": user},
        },
    });
    if (data !== undefined) {
        return Promise.resolve(data.partyDetails?.[0]);
    } else {
        return Promise.resolve(undefined);
    }
}

export async function createUser(user: string, party: string): Promise<string> {
    const resp = await client.POST("/v2/users", {
        body: {
            user: {
                id: user,
                primaryParty: party,
                isDeactivated: false,
                identityProviderId: ""
            },
            rights: [
                {
                    kind: {
                        CanActAs: {
                            value: {
                                party: party
                            }
                        },
                        CanReadAs: {
                            value: {
                                party: party
                            }
                        }
                    }
                }
            ]
        }
    });
    return valueOrError(resp).then(
        data => {
            if (data.user !== undefined) {
                return Promise.resolve(data.user.primaryParty);
            } else {
                return Promise.reject("cannot create user");
            }
        }
    );
}

export async function allocatePartyAndCreateUser(user: string): Promise<string> {
    const userParty = await getUserParty(user);
    if (userParty !== undefined) {
        return Promise.resolve(userParty);
    }
    const party = await allocateParty(user);
    if (party == undefined) {
        return Promise.resolve("could not create party");
    } else {
        return createUser(user, party);
    }
}

async function getParticipantNamespace(): Promise<string> {
    const {data, error} = await client.GET("/v2/parties/participant-id");
    if (data !== undefined) {
        const namespace = data.participantId.split("::")[1];
        return Promise.resolve(namespace);
    } else {
        return Promise.reject(error);
    }
}
