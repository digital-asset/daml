import type {components} from "../generated/api/ledger-api";
import {Iou} from "../generated/model-tests-1.0.0";
import {client, valueOrError} from "./client";

function makeCommands(userParty: string, commands: components["schemas"]["Command"][]): components["schemas"]["JsCommands"] {
    return {
        commands: commands,
        commandId: (Math.random() + 1).toString(36).substring(7),
        userId: "ledger-api-user",
        actAs: [userParty],
        readAs: [userParty],
    };
}

export async function createContract(userParty: string): Promise<components["schemas"]["SubmitAndWaitResponse"]> {
    const iou: Iou.Iou = {
        issuer: userParty,
        owner: userParty,
        currency: "USD",
        amount: "100",
        observers: []
    };

    const command: components["schemas"]["CreateCommand"] = {
        "createArguments": iou,
        "templateId": Iou.Iou.templateId
    };

    const jsCommands = makeCommands(userParty, [{CreateCommand: command}]);
    const resp = await client.POST("/v2/commands/submit-and-wait", {
        body: jsCommands
    });
    return valueOrError(resp);
}

export async function exerciserTransfer(signatory: string, contractId: string, newOwner: string): Promise<components["schemas"]["SubmitAndWaitResponse"]> {
    const transfer: Iou.Iou_Transfer = {
        newOwner: newOwner,
    };

    const command: components["schemas"]["ExerciseCommand"] = {
        contractId: contractId,
        choice: Iou.Iou.Iou_Transfer.choiceName,
        choiceArgument: transfer,
        templateId: Iou.Iou.templateId
    };

    const jsCommands = makeCommands(signatory, [{ExerciseCommand: command}]);
    const resp = await client.POST("/v2/commands/submit-and-wait", {
        body: jsCommands
    });
    return valueOrError(resp);
}

export async function acceptTransfer(signatory: string, contractId: string): Promise<components["schemas"]["SubmitAndWaitResponse"]> {

    const command: components["schemas"]["ExerciseCommand"] = {
        contractId: contractId,
        choice: Iou.IouTransfer.IouTransfer_Accept.choiceName,
        choiceArgument: {},
        templateId: Iou.IouTransfer.templateId
    };

    const jsCommands = makeCommands(signatory, [{ExerciseCommand: command}]);
    const resp = await client.POST("/v2/commands/submit-and-wait", {
        body: jsCommands
    });
    return valueOrError(resp);
}
