// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import createClient from "openapi-fetch";
import {ContractTypeCompanion, Choice} from "@daml/types";
import type {paths, components} from "../codegen/api/ledger-api";
import {Main, packageId as mainPackageId} from "../codegen/codegen-main-0.0.1/lib";
import {Dep, packageId as depPackageId} from "../codegen/codegen-dep-0.0.1/lib";
import * as fs from 'fs'

const client = createClient<paths>({baseUrl: "http://localhost:7575"});

async function createUser(user: string, party: string): Promise<string> {
    let res = await client.POST("/v2/users", {
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

    return assertDefined(`createUser(${user})`, res, res.data?.user?.primaryParty);
}

async function createContract<T extends object, K, I extends string>(
  t: ContractTypeCompanion<T, K, I>,
  party: string, userId: string,
  payload: any,
): Promise<string> {
  let submitCreate = await client.POST("/v2/commands/submit-and-wait-for-transaction", {
    body: {
      commands: {
        commands: [
          {
            CreateCommand: {
              createArguments: payload,
              templateId: t.templateId
            }
          }
        ],
        commandId: `id-${randomId(16)}`,
        userId: userId,
        actAs: [party],
        readAs: [party],
      }
    }
  });

  let submitCreateEvent = assertDefined("submitCreate", submitCreate, (submitCreate.data?.transaction.events ?? [])[0]);
  return assertCreate<{ contractId: string }>("submitCreateEvent", submitCreateEvent).contractId;
}

async function exerciseChoice<T extends object, K, C, R, I extends string>(
  t: ContractTypeCompanion<T, K, I>,
  c: Choice<T, C, R, K>,
  party: string, userId: string,
  contractId: string,
  payload: any,
): Promise<(components["schemas"]["Event"])[]> {
  let submitExercise = await client.POST("/v2/commands/submit-and-wait-for-transaction", {
    body: {
      commands: {
        commands: [
          {
            ExerciseCommand: {
              contractId: contractId,
              choice: c.choiceName,
              choiceArgument: payload,
              templateId: t.templateId,
            }
          }
        ],
        commandId: `id-${randomId(16)}`,
        userId: userId,
        actAs: [party],
        readAs: [party],
      }
    },
    transactionFormat: {
      transactionShape: "TRANSACTION_SHAPE_LEDGER_EFFECTS",
    },
  });

  return assertDefined("submitExercise", submitExercise, submitExercise.data?.transaction.events);
}

function assertDefined<A>(name: string, alt: any, x: A | undefined): A {
  if (x != undefined)
    return x
  else
    throw [`'${name}' is not defined`, alt]
}

function assertCreate<A>(name: string, ev: components["schemas"]["Event"]): components["schemas"]["CreatedEvent"] {
  if ('CreatedEvent' in ev)
    return ev.CreatedEvent
  else
    throw [`'${name}' is not a CreatedEvent`, ev]
}

function randomId(len: number): string {
  return [...Array(len).keys()].map(_ => "0123456789abcdef"[Math.floor(16 * Math.random())]).join("");
}

function assertArchived(contractId: string, events: (components["schemas"]["Event"])[]): string {
  let event = events.find(ev => {
    if ('ArchivedEvent' in ev) return ev.ArchivedEvent.contractId == contractId
    return false;
  });
  if (event != null)
    return (event as { ArchivedEvent: components["schemas"]["ArchivedEvent"] }).ArchivedEvent.contractId;
  else
    throw [`${contractId} was not archived.`, events];
}

(async () => {
  let listResPre = await client.GET("/v2/packages");
  let pre = new Set(assertDefined("listResPre", listResPre, listResPre.data?.packageIds));

  let uploadRes = await client.POST("/v2/dars", {
    body: "./target.dar",
    bodySerializer: body => fs.readFileSync(body),
    params: {
      query: {
        vetAllPackages: true,
      },
    },
  });

  let listResPost = await client.GET("/v2/packages");
  let post = assertDefined("listResPost", listResPost, listResPost.data?.packageIds);

  let aliceSuffix = randomId(16);
  let allocateParty = await client.POST("/v2/parties", {
    body: {
      partyIdHint: `alice-${aliceSuffix}`,
      identityProviderId: "",
      synchronizerId: "",
      userId: "",
    }
  });

  let alice: string = assertDefined("allocateParty(alice)", allocateParty, allocateParty.data?.partyDetails?.party);
  let aliceUser = await createUser(`Alice-${aliceSuffix}`, alice);

  let depContractId = await createContract(Dep.DepData, alice, aliceUser, {
    depParty: alice,
    value: 5,
  });

  let mainContractId = await createContract(Main.MainData, alice, aliceUser, {
    mainParty: alice,
    value: 7,
    dep: depContractId,
  });

  let exerciseEvents = await exerciseChoice(Main.MainData, Main.MainData.Consume, alice, aliceUser, mainContractId, {});
  assertArchived(mainContractId, exerciseEvents);
  assertArchived(depContractId, exerciseEvents);
})().catch(e => {
  console.log("Error", e);
  throw e;
});

