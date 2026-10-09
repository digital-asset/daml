import type {components} from '../generated/api/ledger-api';
import {Iou} from '../generated/model-tests-1.0.0';
import Table from "cli-table3";
import {client} from './client';
import {allocatePartyAndCreateUser} from "./user";
import {acceptTransfer, createContract, exerciserTransfer} from "./commands";

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

    const {data, error} = await client.POST("/v2/state/active-contracts", {
        body: filter
    });
    if (data === undefined)
        return Promise.reject(error);
    else {
        const contracts: components["schemas"]["CreatedEvent"][] = data.map((res) => res.contractEntry).filter((res) => "JsActiveContract" in res).map(
            (res) => res.JsActiveContract.createdEvent
        );
        return Promise.resolve(contracts);
    }
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
    const {data, error} = await client.POST("/v2/updates", {
        body: filter
    });
    if (data === undefined)
        return Promise.reject(error);
    else {
        const updatedContracts: components["schemas"]["CreatedEvent"][] = contracts;
        data.map((res) => res.update)
            .filter((res) => "Transaction" in res).map(
            (res) => res.Transaction as components["schemas"]["Transaction"]
        )
            .map((res) => res.value)
            .filter((res) => res.events !== undefined)
            .flatMap((res) => res.events)
            .filter((res) => res !== undefined)
            .forEach((res) => {
                if ("CreatedEvent" in res){
                    updatedContracts.push(res.CreatedEvent as components["schemas"]["CreatedEvent"]);
                } else if ("ArchivedEvent" in res) {
                    const contractId = res.ArchivedEvent.contractId;
                    const index = updatedContracts.findIndex(c => c.contractId === contractId);
                    if (index >= 0) {
                        updatedContracts.splice(index, 1);
                    }
                }
            })

        return Promise.resolve(updatedContracts);
    }
}

function showAcs(created: components["schemas"]["CreatedEvent"][],
                 headers: string[],
                 values: (arg: any) => any[],
) {
    const table = new Table({
        head: headers,
    });
    created.map(c => c.createArgument as any).forEach(c => table.push(values(c)));
    console.log(table.toString());
}

scenario();
