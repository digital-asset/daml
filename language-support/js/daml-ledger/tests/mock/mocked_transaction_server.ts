// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Server, ServerUnaryCall, Metadata, ServiceError, ServerWriteableStream } from 'grpc';

import * as grpc from 'daml-grpc';

export class MockedTransactionServer extends Server {

    constructor(ledgerId: string, transactions: grpc.GetTransactionsResponse[]) {
        super();

        const ledgerIdentityResponse: grpc.GetLedgerIdentityResponse = new grpc.GetLedgerIdentityResponse();
        ledgerIdentityResponse.setLedgerId(ledgerId);

        this.addService(grpc.LedgerIdentityService, {
            getLedgerIdentity(_call: ServerUnaryCall<grpc.GetLedgerIdentityRequest>, callback: (error: ServiceError | null, value: grpc.GetLedgerIdentityResponse | null, trailer?: Metadata, flags?: number) => void): void {
                callback(null, ledgerIdentityResponse);
            }
        });

        this.addService(grpc.TransactionService, {
            getTransactions(call: ServerWriteableStream<grpc.GetTransactionsRequest>): void {
                for (const tx of transactions) {
                    call.write(tx);
                }
                call.end();
            },
            getTransactionTrees(call: ServerWriteableStream<grpc.GetTransactionsRequest>): void {
                call.end();
            },
            getTransactionByEventId(_call: ServerUnaryCall<grpc.GetTransactionByEventIdRequest>, callback: (error: ServiceError | null, value: grpc.GetTransactionResponse | null, trailer?: Metadata, flags?: number) => void): void {
                callback(null, null);
            },
            getTransactionById(_call: ServerUnaryCall<grpc.GetTransactionByIdRequest>, callback: (error: ServiceError | null, value: grpc.GetTransactionResponse | null, trailer?: Metadata, flags?: number) => void): void {
                callback(null, null);
            },
            getLedgerEnd(_call: ServerUnaryCall<grpc.GetLedgerEndRequest>, callback: (error: ServiceError | null, value: grpc.GetLedgerEndResponse | null, trailer?: Metadata, flags?: number) => void): void {
                callback(null, null);
            }
        });

    }

}