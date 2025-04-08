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
    const contracts = await acs(alice, Iou.Iou.templateId, ledgerOffset1);
    showAcs(contracts, ["owner", "amount", "currency"], c => [c.owner, c.amount, c.currency]);
    if (contracts.length > 0) {
        const last = contracts[contracts.length - 1];
        const contractId = last.contractId;
        console.log("Alice transfers iou to Bob");
        const ledgerOffset2 = (await exerciserTransfer(alice, contractId, bob)).completionOffset;
        const contracts2 = await acs(bob, Iou.IouTransfer.templateId, ledgerOffset2);
        showAcs(contracts2, ["newOwner", "Iou", "currency", "owner"], c => [c.newOwner, c.iou.amount, c.iou.currency, c.iou.owner]);

        if (contracts2.length > 0) {
            const transferContractId = contracts2[0].contractId;
            console.log("Bob accepts transfer");
            const lastUpdate = await acceptTransfer(bob, transferContractId)

            const finalContracts = await acs(alice, Iou.Iou.templateId, lastUpdate.completionOffset);
            showAcs(finalContracts, ["owner", "amount", "currency"], c => [c.owner, c.amount, c.currency]);

            console.log("End of scenario");
        }
    }

}

async function acs(userParty: string, templateId: string, ledger_offset: number): Promise<components["schemas"]["CreatedEvent"][]> {
    const templateFilter: components["schemas"]["TemplateFilter"] = {
        value: {
            templateId: templateId,
            includeCreatedEventBlob: false,
        }
    };
    const filter: components["schemas"]["GetActiveContractsRequest"] = {
        "filter": {
            "filtersByParty": {},
            "filtersForAnyParty": {
                "cumulative": [{
                    identifierFilter: {
                        TemplateFilter: templateFilter
                    }
                }]
            }
        }, "verbose": false,
        "activeAtOffset": ledger_offset,
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




