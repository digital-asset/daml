// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.TransactionsClient;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory;
import com.digitalasset.ledger.api.v1.TransactionServiceGrpc;
import com.digitalasset.ledger.api.v1.TransactionServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Set;
import java.util.concurrent.Future;

public class TransactionClientImpl implements TransactionsClient {
    private final String ledgerId;
    private final TransactionServiceGrpc.TransactionServiceStub serviceStub;
    private final TransactionServiceGrpc.TransactionServiceFutureStub serviceFutureStub;
    private final ExecutionSequencerFactory sequencerFactory;

    public TransactionClientImpl(String ledgerId, Channel channel, ExecutionSequencerFactory sequencerFactory) {
        this.ledgerId = ledgerId;
        this.sequencerFactory = sequencerFactory;
        serviceStub = TransactionServiceGrpc.newStub(channel);
        serviceFutureStub = TransactionServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Flowable<Transaction> getTransactions(LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose) {
        TransactionServiceOuterClass.GetTransactionsRequest request = new GetTransactionsRequest(ledgerId, begin, end, filter, verbose).toProto();
        return extractTransactions(request);
    }

    @Override
    public Flowable<Transaction> getTransactions(LedgerOffset begin, TransactionFilter filter, boolean verbose) {
        TransactionServiceOuterClass.GetTransactionsRequest request = new GetTransactionsRequest(ledgerId, begin, filter, verbose).toProto();
        return extractTransactions(request);
    }

    private Flowable<Transaction> extractTransactions(TransactionServiceOuterClass.GetTransactionsRequest request) {
        return ClientPublisherFlowable
                .create(request, serviceStub::getTransactions, sequencerFactory)
                .map(GetTransactionsResponse::fromProto)
                .concatMapIterable(GetTransactionsResponse::getTransactions);
    }


    @Override
    public Flowable<TransactionTree> getTransactionsTrees(LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose) {
        TransactionServiceOuterClass.GetTransactionsRequest request = new GetTransactionsRequest(ledgerId, begin, end, filter, verbose).toProto();
        return extractTransactionTrees(request);
    }

    @Override
    public Flowable<TransactionTree> getTransactionsTrees(LedgerOffset begin, TransactionFilter filter, boolean verbose) {
        TransactionServiceOuterClass.GetTransactionsRequest request = new GetTransactionsRequest(ledgerId, begin, filter, verbose).toProto();
        return extractTransactionTrees(request);
    }

    private Flowable<TransactionTree> extractTransactionTrees(TransactionServiceOuterClass.GetTransactionsRequest request) {
        return ClientPublisherFlowable
                .create(request, serviceStub::getTransactionTrees, sequencerFactory)
                .map(GetTransactionTreesResponse::fromProto)
                .concatMapIterable(GetTransactionTreesResponse::getTransactions);
    }


    @Override
    public Single<TransactionTree> getTransactionByEventId(String eventId, Set<String> requestingParties) {
        TransactionServiceOuterClass.GetTransactionByEventIdRequest request = TransactionServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEventId(eventId)
                .addAllRequestingParties(requestingParties)
                .build();
        return extractTransactionTree(serviceFutureStub.getTransactionByEventId(request));
    }

    @Override
    public Single<TransactionTree> getTransactionById(String transactionId, Set<String> requestingParties) {
        TransactionServiceOuterClass.GetTransactionByIdRequest request = TransactionServiceOuterClass.GetTransactionByIdRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setTransactionId(transactionId)
                .addAllRequestingParties(requestingParties)
                .build();
        return extractTransactionTree(serviceFutureStub.getTransactionById(request));
    }

    private Single<TransactionTree> extractTransactionTree(Future<TransactionServiceOuterClass.GetTransactionResponse> future) {
        return Single
                .fromFuture(future)
                .map(GetTransactionResponse::fromProto)
                .map(GetTransactionResponse::getTransaction);
    }

    @Override
    public Single<LedgerOffset> getLedgerEnd() {
        TransactionServiceOuterClass.GetLedgerEndRequest request = TransactionServiceOuterClass.GetLedgerEndRequest.newBuilder().setLedgerId(ledgerId).build();
        return Single
                .fromFuture(serviceFutureStub.getLedgerEnd(request))
                .map(GetLedgerEndResponse::fromProto)
                .map(GetLedgerEndResponse::getOffset);
    }
}
