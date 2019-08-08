import * as path from 'path';
import * as fs from 'fs';
import * as cp from 'child_process';
import * as vscode from 'vscode';

interface VisualTaskDefinition extends vscode.TaskDefinition {}

export class VisualTaskProvider implements vscode.TaskProvider{

    private tasks: vscode.Task[] | undefined;
    static VisualTaskProviderType: string = 'custombuildscript';

    public provideTasks(token?: vscode.CancellationToken): vscode.ProviderResult<vscode.Task[]> {
        return [this.getTask()];
    }

    public resolveTask(task: vscode.Task, token?: vscode.CancellationToken): vscode.ProviderResult<vscode.Task> {
        throw new Error("Method not implemented.");
    }

    private getTask(definition?: VisualTaskDefinition):
    vscode.Task{
		if (definition === undefined) {
			definition = {
				type: VisualTaskProvider.VisualTaskProviderType,
			};
        }
		return new vscode.Task(definition, vscode.TaskScope.Workspace, "visual command" ,
            VisualTaskProvider.VisualTaskProviderType,
            new vscode.ShellExecution("daml clean && daml build && daml damlc visual .daml/dist/*dar > visual.dot")
            );
	}
}