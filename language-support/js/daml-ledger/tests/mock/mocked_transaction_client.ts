// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as sinon from 'sinon';

import * as grpc from 'daml-grpc';

import { CallOptions, ClientReadableStream, ClientUnaryCall, Metadata } from 'grpc';
import { MockedClientReadableStream } from './client_readable_stream';
import { MockedClientUnaryCall } from './client_unary_call';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';

export class MockedTransactionClient implements grpc.ITransactionClient {

    // private readonly ledgerId: string
    private readonly latestRequestSpy: sinon.SinonSpy

    private readonly ledgerEndResponse = new grpc.GetLedgerEndResponse();
    private initEmptyLedgerEndResponse() {
        const offset = new grpc.LedgerOffset();
        offset.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_BEGIN);
        this.ledgerEndResponse.setOffset(offset);
    }
    private readonly transactionResponse = new grpc.GetTransactionResponse();
    private initEmptyTransactionResponse() {
        const effectiveAt = new Timestamp();
        effectiveAt.setSeconds(0);
        effectiveAt.setNanos(0);
        const tree = new grpc.TransactionTree();
        tree.setEffectiveAt(effectiveAt);
        tree.setOffset('mock');
        tree.setTransactionId('mock');
        this.transactionResponse.setTransaction(tree);
    }

    constructor(_ledgerId: string, latestRequestSpy: sinon.SinonSpy) {
        this.latestRequestSpy = latestRequestSpy;
        this.initEmptyLedgerEndResponse();
        this.initEmptyTransactionResponse();
    }

    getTransactions(request: grpc.GetTransactionsRequest, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetTransactionsResponse>;
    getTransactions(request: grpc.GetTransactionsRequest, metadata?: Metadata | undefined, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetTransactionsResponse>;
    getTransactions(request: grpc.GetTransactionsRequest, _metadata?: any, _options?: any) {
        this.latestRequestSpy(request);
        return MockedClientReadableStream.with([]);
    }

    getTransactionTrees(request: grpc.GetTransactionsRequest, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetTransactionTreesResponse>;
    getTransactionTrees(request: grpc.GetTransactionsRequest, metadata?: Metadata | undefined, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.GetTransactionTreesResponse>;
    getTransactionTrees(request: grpc.GetTransactionsRequest, _metadata?: any, _options?: any) {
        this.latestRequestSpy(request);
        return MockedClientReadableStream.with([]);
    }

    getTransactionByEventId(request: grpc.GetTransactionByEventIdRequest, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionByEventId(request: grpc.GetTransactionByEventIdRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionByEventId(request: grpc.GetTransactionByEventIdRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionByEventId(request: grpc.GetTransactionByEventIdRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options) : callback;
        setImmediate(() => cb(null, this.transactionResponse));
        return MockedClientUnaryCall.Instance;
    }

    getTransactionById(request: grpc.GetTransactionByIdRequest, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionById(request: grpc.GetTransactionByIdRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionById(request: grpc.GetTransactionByIdRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetTransactionResponse) => void): ClientUnaryCall;
    getTransactionById(request: grpc.GetTransactionByIdRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options) : callback;
        setImmediate(() => cb(null, this.transactionResponse));
        return MockedClientUnaryCall.Instance;
    }

    getLedgerEnd(request: grpc.GetLedgerEndRequest, callback: (error: Error | null, response: grpc.GetLedgerEndResponse) => void): ClientUnaryCall;
    getLedgerEnd(request: grpc.GetLedgerEndRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetLedgerEndResponse) => void): ClientUnaryCall;
    getLedgerEnd(request: grpc.GetLedgerEndRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetLedgerEndResponse) => void): ClientUnaryCall;
    getLedgerEnd(request: grpc.GetLedgerEndRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options) : callback;
        setImmediate(() => cb(null, this.ledgerEndResponse));
        return MockedClientUnaryCall.Instance;
    }

}