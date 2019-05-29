// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

'use strict';

import * as assert from 'assert';
import * as cp from 'child_process';
import * as types from 'vscode-languageserver-types';
import * as events from 'events';
import {
    createMessageConnection, Logger, Trace, Tracer, MessageConnection,
    NotificationType, NotificationHandler
} from 'vscode-jsonrpc';

import {
    InitializeRequest, InitializeParams, InitializeResult,
    LogMessageNotification,
    DefinitionRequest,
    ReferencesRequest,
    DidChangeTextDocumentNotification,
    DidOpenTextDocumentNotification,
    DidCloseTextDocumentNotification,
    CodeLensRequest,
    PublishDiagnosticsNotification,
    PublishDiagnosticsParams
} from 'vscode-languageserver-protocol';

import * as Rx from 'rxjs';
import { filter } from 'rxjs/operators';

export class DamlConnection {
    private connection: MessageConnection;
    private openDocuments: string[] = [];
    private process: cp.ChildProcess;
    private command: { command: string, args: Array<string> };
    private tracing: boolean = false;
    public workspaceValidations: Rx.Observable<WorkspaceValidationsParams>;
    public allValidationsPassed: Rx.Observable<{}>
    public diagnostics: Rx.Observable<PublishDiagnosticsParams>;

    public constructor(command: { command : string, args : Array<string> }, tracing: boolean) {
        this.command = command;
        this.tracing = tracing;
    }

    public start() {
        this.process = cp.spawn(this.command.command, this.command.args, {});
        this.connection = createMessageConnection(
            this.process.stdout, this.process.stdin, new ConsoleLogger()
        );
        if (this.tracing) {
            this.process.stderr.on('readable', () => {
                console.log(`STDERR: ${this.process.stderr.read()}`);
            });

            this.connection.onNotification(LogMessageNotification.type,
                msg => console.log(`MESSAGE: ${msg.message}`));

            this.connection.onUnhandledNotification(note => {
                console.log("Unhandled notification: ", note);
            });

            this.connection.trace(Trace.Verbose, new ConsoleTracer());
        } else {
            // drain stderr in order to not block damlc.
            this.process.stderr.on('readable', () => { this.process.stderr.read() });
        }

        this.workspaceValidations = Rx.Observable.create((observer: Rx.Observer<WorkspaceValidationsParams>) => {
            this.connection.onNotification(
                DamlWorkspaceValidationsNotification.type,
                (validation) => observer.next(validation)
            );
        });

        this.allValidationsPassed =
            this.workspaceValidations.pipe(filter((val) => val.finishedValidations == val.totalValidations));

        this.diagnostics = Rx.Observable.create((observer: Rx.Observer<PublishDiagnosticsParams>) => {
            this.connection.onNotification(
                PublishDiagnosticsNotification.type,
                (diagnostic) => observer.next(diagnostic)
            );
        });
        this.connection.listen();
    }

    public stop() {
        this.connection.dispose();
        this.process.kill
    }

    public onNotification<P>(type: NotificationType<P, void>, handler: NotificationHandler<P>) {
        this.connection.onNotification(type, handler);
    }

    public initialize(rootPath: string): Thenable<InitializeResult>  {
        return this.connection.sendRequest(InitializeRequest.type, <InitializeParams> {
            processId: process.pid,
            rootPath: '/tmp',
            capabilities: {},
            initializationOptions: null,
        });
    }

    public openDocument(uri: string, text: string) {
        this.openDocuments.push(uri);
        this.connection.sendNotification(
            DidOpenTextDocumentNotification.type,
            {
                textDocument: {
                    uri: uri,
                    languageId: 'daml',
                    version: 0,
                    text: text
                }
            }
        );
    }

    public changeDocument(uri: string, version: number, text: string) {
        this.connection.sendNotification(
            DidChangeTextDocumentNotification.type,
            {
                textDocument: { uri: uri, version: version },
                contentChanges: [ {
                    range: undefined,
                    rangeLength: undefined,
                    text: text
                }]
            }
        );
    }

    public closeDocument(uri: string) {
        this.openDocuments = this.openDocuments.filter(u => u != uri);
        this.connection.sendNotification(
            DidCloseTextDocumentNotification.type,
            { textDocument: { uri: uri } }
        );
    }

    public closeAllDocuments() {
        this.openDocuments.forEach(uri => {
            this.connection.sendNotification(
                DidCloseTextDocumentNotification.type,
                { textDocument: { uri: uri } }
            );
        });
        this.openDocuments = [];
    }

    public codeLens(uri: string): Thenable<types.CodeLens[]> {
        return this.connection.sendRequest(
            CodeLensRequest.type,
            { textDocument: { uri: uri }}
        );
    }

    /**
     * Untyped send request.
     */
    public sendRequest(method, payload): Thenable<any> {
        return this.connection.sendRequest(method, payload);
    }

    /**
     * Search for all references pointing to a symbol at a position in a document.
     * @param uri URI of the document.
     * @param position Position in the document.
     */
    public references(uri : string, position : types.Position) : Thenable<types.Location[]> {
        return this.connection.sendRequest(
            ReferencesRequest.type,
            {
                context: { includeDeclaration: true },
                textDocument: { uri: uri },
                position: { line: position.line, character: position.character }
            }
        );
    }

    /**
     * Search for the definition of a symbol at a position in a document.
     * @param uri URI of the document.
     * @param position Position in the document.
     */
    public definition(uri: string, position: types.Position) {
        return this.connection.sendRequest(
          DefinitionRequest.type,
          { textDocument: { uri: uri }, position: position }
        );
    }
}

class ConsoleLogger implements Logger {
    public error(message: string) : void { console.error(message); }
    public warn(message: string) : void { console.warn(message); }
    public info(message: string) : void { console.info(message); }
    public log(message: string) : void { console.log(message); }
}

class ConsoleTracer implements Tracer {
    public log(message: string, data?: string) {
      console.log(`TRACE: ${message} (${data})`);
    }
}

export interface WorkspaceValidationsParams {
    /** Tracks the number of validations we have already finished. */
    finishedValidations : number;
    /** Tracks the number of total validation steps we need to perform. */
    totalValidations    : number;
}

namespace DamlWorkspaceValidationsNotification {
    export let type = new NotificationType<WorkspaceValidationsParams, void>(
      'daml/workspace/validations'
    );
}
