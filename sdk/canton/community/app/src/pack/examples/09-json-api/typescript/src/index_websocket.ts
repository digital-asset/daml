import type {components} from '../generated/api/ledger-api';
import {Iou} from '../generated/model-tests-1.0.0';
import {connectWs} from './websocket_client';
import {allocatePartyAndCreateUser} from "./user";
import {acceptTransfer, createContract, exerciserTransfer} from "./commands";
import {showAcs} from "./logging";

//Runs scenario with Iou creation and transfer
async function scenario() {
    const alice = await allocatePartyAndCreateUser("Alice");
    const bob = await allocatePartyAndCreateUser("Bob");
    console.log("Alice creates contract");
    const ledgerOffset1 = (await createContract(alice)).completionOffset;
    console.log(`Ledger offset: ${ledgerOffset1}`)
    const transactionContracts = await acs(alice, Iou.Iou.templateId, ledgerOffset1);
    showAcs(transactionContracts, ["owner", "amount", "currency"], c => [c.owner, c.amount, c.currency]);
    let iouContractsCreated = false;
    if (transactionContracts.length > 0) {
        const last = transactionContracts[transactionContracts.length - 1];
        const contractId = last.contractId;
        console.log("Alice transfers iou to Bob");
        const ledgerOffset2 = (await exerciserTransfer(alice, contractId, bob)).completionOffset;
        const retrievedIouContracts = await acs(bob, Iou.IouTransfer.templateId, ledgerOffset2);
        showAcs(retrievedIouContracts, ["newOwner", "Iou", "currency", "owner"], c => [c.newOwner, c.iou.amount, c.iou.currency, c.iou.owner]);
        transactionContracts.push(...retrievedIouContracts);
        iouContractsCreated = true;
    }
    if (iouContractsCreated) {
        const iouContractId = transactionContracts[transactionContracts.length - 1].contractId;
        console.log("Bob accepts transfer");
        const lastUpdate = await acceptTransfer(bob, iouContractId)
        const ledgerOffset3 = lastUpdate.completionOffset;
        const transactionUpdatedContracts = await updateContracts(ledgerOffset1, ledgerOffset3, alice, Iou.Iou.templateId, transactionContracts);
        showAcs(transactionUpdatedContracts
                .filter((contract) => !("iou" in (contract.createArgument as components["schemas"]["CreatedEvent"]))),
            ["owner", "amount", "currency"], c => [c.owner, c.amount, c.currency]);
    }
    console.log("End of scenario");
}

function eventFormat(userParty: string, templateId: string): components["schemas"]["EventFormat"] {
    const templateFilter: components["schemas"]["TemplateFilter"] = {
        value: {
            templateId: templateId,
            includeCreatedEventBlob: false,
        }
    };
    return {
        filtersByParty: {
            [userParty]: {
                cumulative: [{
                    identifierFilter: {
                        TemplateFilter: templateFilter
                    }
                }]
            }
        },
        verbose: false
    };
}

async function acs(userParty: string, templateId: string, ledger_offset: number): Promise<components["schemas"]["CreatedEvent"][]> {
    const filter: components["schemas"]["GetActiveContractsRequest"] = {
        eventFormat: eventFormat(userParty, templateId),
        verbose: false,
        activeAtOffset: ledger_offset,
    };
    return new Promise<components["schemas"]["CreatedEvent"][]>((resolve, reject) => {
        let contracts: components["schemas"]["CreatedEvent"][] = [];
        return connectWs("/v2/state/active-contracts").then(ws => {
            ws.send(JSON.stringify(filter));
            ws.onmessage = function (msg: any) {
                const data = JSON.parse(msg.data) as components["schemas"]["JsGetActiveContractsResponse"];
                const contractEntry = (data.contractEntry as components["schemas"]["JsContractEntry"]);
                if ("JsActiveContract" in contractEntry){
                    contracts.push(contractEntry.JsActiveContract.createdEvent as components["schemas"]["CreatedEvent"]);
                }
            }
            ws.onclose = function () {
                resolve(contracts);
            }
        })
    })
}

async function updateContracts(startOffset: number, endOffset: number, userParty: string, templateId: string, contracts: components["schemas"]["CreatedEvent"][]): Promise<components["schemas"]["CreatedEvent"][]> {
    const filter: components["schemas"]["GetUpdatesRequest"] = {
        beginExclusive: startOffset,
        endInclusive: endOffset,
        updateFormat: {
            includeTransactions: {
                eventFormat: eventFormat(userParty, templateId),
                transactionShape: 'TRANSACTION_SHAPE_ACS_DELTA',
            },
        },
        verbose: false,
    };

    return new Promise<components["schemas"]["CreatedEvent"][]>((resolve, reject) => {
        return connectWs("/v2/updates").then(ws => {
            ws.send(JSON.stringify(filter));
            ws.onmessage = function (msg: any) {
                const data = JSON.parse(msg.data) as components["schemas"]["JsGetUpdateResponse"];
                const update = (data as components["schemas"]["JsGetUpdateResponse"]).update;
                if (update !== undefined && "Transaction" in update) {
                    update.Transaction.value?.events?.forEach((event: any) => {
                        if ("CreatedEvent" in event) {
                            contracts.push(event.CreatedEvent as components["schemas"]["CreatedEvent"]);
                        } else if ("ArchivedEvent" in event) {
                            const contractId = event.ArchivedEvent.contractId;
                            const index = contracts.findIndex(c => c.contractId === contractId);
                            if (index >= 0) {
                                contracts.splice(index, 1);
                            }
                        }
                    })
                }
            }
            ws.onclose = function () {
                resolve(contracts);
            }
        })
    })

}



scenario();
