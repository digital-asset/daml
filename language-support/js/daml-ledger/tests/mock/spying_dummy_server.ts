// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Server, ServerUnaryCall, Metadata, ServiceError, ServerWriteableStream } from 'grpc';

import * as grpc from 'daml-grpc';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import { SinonSpy } from 'sinon';

export class SpyingDummyServer extends Server {

    constructor(ledgerId: string, spy: SinonSpy) {
        super();

        const ledgerIdentityResponse: grpc.GetLedgerIdentityResponse = new grpc.GetLedgerIdentityResponse();
        ledgerIdentityResponse.setLedgerId(ledgerId);

        const empty = new Empty();

        const offset = new grpc.LedgerOffset();
        offset.setBoundary(grpc.LedgerOffset.LedgerBoundary.LEDGER_BEGIN);

        const completionEndResponse = new grpc.CompletionEndResponse();
        completionEndResponse.setOffset(offset);

        const listPackagesResponse = new grpc.ListPackagesResponse();

        const getPackageResponse = new grpc.GetPackageResponse();

        const getPackageStatusResponse = new grpc.GetPackageStatusResponse();

        const effectiveAt = new Timestamp();
        const transactionTree = new grpc.TransactionTree();
        transactionTree.setEffectiveAt(effectiveAt);
        const getTransactionResponse = new grpc.GetTransactionResponse();
        getTransactionResponse.setTransaction(transactionTree);

        const getLedgerEndResponse = new grpc.GetLedgerEndResponse();
        getLedgerEndResponse.setOffset(offset);

        this.addService(grpc.ActiveContractsService, {
            getActiveContracts(call: ServerWriteableStream<grpc.GetActiveContractsRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            }
        });

        this.addService(grpc.CommandService, {
            submitAndWait(call: ServerUnaryCall<grpc.SubmitAndWaitRequest>, callback: (error: ServiceError | null, value: Empty | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getCommands()!.getLedgerId());
                callback(null, empty);
            }
        });

        this.addService(grpc.CommandCompletionService, {
            completionStream(call: ServerWriteableStream<grpc.CompletionStreamRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            },
            completionEnd(call: ServerUnaryCall<grpc.CompletionEndRequest>, callback: (error: ServiceError | null, value: grpc.CompletionEndResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, completionEndResponse);
            }
        });

        this.addService(grpc.CommandSubmissionService, {
            submit(call: ServerUnaryCall<grpc.SubmitRequest>, callback: (error: ServiceError | null, value: Empty | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getCommands()!.getLedgerId());
                callback(null, empty);
            }
        });

        this.addService(grpc.LedgerIdentityService, {
            getLedgerIdentity(_call: ServerUnaryCall<grpc.GetLedgerIdentityRequest>, callback: (error: ServiceError | null, value: grpc.GetLedgerIdentityResponse | null, trailer?: Metadata, flags?: number) => void): void {
                callback(null, ledgerIdentityResponse);
            }
        });

        this.addService(grpc.PackageService, {
            listPackages(call: ServerUnaryCall<grpc.ListPackagesRequest>, callback: (error: ServiceError | null, value: grpc.ListPackagesResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, listPackagesResponse);
            },
            getPackage(call: ServerUnaryCall<grpc.GetPackageRequest>, callback: (error: ServiceError | null, value: grpc.GetPackageResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, getPackageResponse);
            },
            getPackageStatus(call: ServerUnaryCall<grpc.GetPackageStatusRequest>, callback: (error: ServiceError | null, value: grpc.GetPackageStatusResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, getPackageStatusResponse);
            }
        });

        this.addService(grpc.LedgerConfigurationService, {
            getLedgerConfiguration(call: ServerWriteableStream<grpc.GetLedgerConfigurationRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            }
        });

        this.addService(grpc.testing.TimeService, {
            getTime(call: ServerWriteableStream<grpc.testing.GetTimeRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            },
            setTime(call: ServerUnaryCall<grpc.testing.SetTimeRequest>, callback: (error: ServiceError | null, value: Empty | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, empty);
            }
        });

        this.addService(grpc.TransactionService, {
            getTransactions(call: ServerWriteableStream<grpc.GetTransactionsRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            },
            getTransactionTrees(call: ServerWriteableStream<grpc.GetTransactionsRequest>): void {
                spy(call.request.getLedgerId());
                call.end();
            },
            getTransactionByEventId(call: ServerUnaryCall<grpc.GetTransactionByEventIdRequest>, callback: (error: ServiceError | null, value: grpc.GetTransactionResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, getTransactionResponse);
            },
            getTransactionById(call: ServerUnaryCall<grpc.GetTransactionByIdRequest>, callback: (error: ServiceError | null, value: grpc.GetTransactionResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, getTransactionResponse);
            },
            getLedgerEnd(call: ServerUnaryCall<grpc.GetLedgerEndRequest>, callback: (error: ServiceError | null, value: grpc.GetLedgerEndResponse | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, getLedgerEndResponse);
            }
        });

        this.addService(grpc.testing.ResetService, {
            reset(call: ServerUnaryCall<grpc.testing.SetTimeRequest>, callback: (error: ServiceError | null, value: Empty | null, trailer?: Metadata, flags?: number) => void): void {
                spy(call.request.getLedgerId());
                callback(null, empty);
            }
        });

    }

}