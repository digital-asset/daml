// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.TransactionServiceGrpc;
import com.daml.ledger.api.v1.TransactionServiceOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.ContractUtil;
import com.daml.ledger.rxjava.TransactionsClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

public final class TransactionClientImpl implements TransactionsClient {
  private final String ledgerId;
  private final TransactionServiceGrpc.TransactionServiceStub serviceStub;
  private final TransactionServiceGrpc.TransactionServiceFutureStub serviceFutureStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public TransactionClientImpl(
      String ledgerId,
      Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.sequencerFactory = sequencerFactory;
    this.serviceStub =
        StubHelper.authenticating(TransactionServiceGrpc.newStub(channel), accessToken);
    this.serviceFutureStub =
        StubHelper.authenticating(TransactionServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Flowable<Transaction> extractTransactions(
      TransactionServiceOuterClass.GetTransactionsRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getTransactions,
            sequencerFactory)
        .map(GetTransactionsResponse::fromProto)
        .concatMapIterable(GetTransactionsResponse::getTransactions);
  }

  private Flowable<Transaction> getTransactions(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionsRequest request =
        new GetTransactionsRequest(ledgerId, begin, end, filter, verbose).toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<Transaction> getTransactions(
      LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose) {
    return getTransactions(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<Transaction> getTransactions(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken) {
    return getTransactions(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private <Ct> Flowable<Ct> getContracts(
      ContractUtil<Ct> contractUtil,
      LedgerOffset begin,
      Set<String> parties,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionFilter filter = contractUtil.transactionFilter(parties);
    Flowable<Transaction> transactions = getTransactions(begin, filter, verbose, accessToken);
    Flowable<CreatedEvent> createdEvents =
        transactions
            .concatMapIterable(Transaction::getEvents)
            .flatMap(
                event ->
                    (event instanceof CreatedEvent)
                        ? Flowable.just((CreatedEvent) event)
                        : Flowable.empty());
    return createdEvents.map(contractUtil::toContract);
  }

  @Override
  public <Ct> Flowable<Ct> getContracts(
      ContractUtil<Ct> contractUtil, LedgerOffset begin, Set<String> parties, boolean verbose) {
    return getContracts(contractUtil, begin, parties, verbose, Optional.empty());
  }

  @Override
  public <Ct> Flowable<Ct> getContracts(
      ContractUtil<Ct> contractUtil,
      LedgerOffset begin,
      Set<String> parties,
      boolean verbose,
      String accessToken) {
    return getContracts(contractUtil, begin, parties, verbose, Optional.of(accessToken));
  }

  private Flowable<Transaction> getTransactions(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionsRequest request =
        new GetTransactionsRequest(ledgerId, begin, filter, verbose).toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<Transaction> getTransactions(
      LedgerOffset begin, TransactionFilter filter, boolean verbose) {
    return getTransactions(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<Transaction> getTransactions(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, String accessToken) {
    return getTransactions(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionTree> extractTransactionTrees(
      TransactionServiceOuterClass.GetTransactionsRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getTransactionTrees,
            sequencerFactory)
        .map(GetTransactionTreesResponse::fromProto)
        .concatMapIterable(GetTransactionTreesResponse::getTransactions);
  }

  private Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionsRequest request =
        new GetTransactionsRequest(ledgerId, begin, filter, verbose).toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, TransactionFilter filter, boolean verbose) {
    return getTransactionsTrees(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, TransactionFilter filter, boolean verbose, String accessToken) {
    return getTransactionsTrees(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionsRequest request =
        new GetTransactionsRequest(ledgerId, begin, end, filter, verbose).toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin, LedgerOffset end, TransactionFilter filter, boolean verbose) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      LedgerOffset begin,
      LedgerOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private Single<TransactionTree> extractTransactionTree(
      Future<TransactionServiceOuterClass.GetTransactionResponse> future) {
    return Single.fromFuture(future)
        .map(GetTransactionResponse::fromProto)
        .map(GetTransactionResponse::getTransaction);
  }

  private Single<TransactionTree> getTransactionByEventId(
      String eventId, Set<String> requestingParties, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionByEventIdRequest request =
        TransactionServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setEventId(eventId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransactionTree(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getTransactionByEventId(request));
  }

  @Override
  public Single<TransactionTree> getTransactionByEventId(
      String eventId, Set<String> requestingParties) {
    return getTransactionByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTree> getTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getTransactionByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<TransactionTree> getTransactionById(
      String transactionId, Set<String> requestingParties, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionByIdRequest request =
        TransactionServiceOuterClass.GetTransactionByIdRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setTransactionId(transactionId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransactionTree(
        StubHelper.authenticating(this.serviceFutureStub, accessToken).getTransactionById(request));
  }

  @Override
  public Single<TransactionTree> getTransactionById(
      String transactionId, Set<String> requestingParties) {
    return getTransactionById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTree> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getTransactionById(transactionId, requestingParties, Optional.of(accessToken));
  }

  private Single<Transaction> extractTransaction(
      Future<TransactionServiceOuterClass.GetFlatTransactionResponse> future) {
    return Single.fromFuture(future)
        .map(GetFlatTransactionResponse::fromProto)
        .map(GetFlatTransactionResponse::getTransaction);
  }

  private Single<Transaction> getFlatTransactionByEventId(
      String eventId, Set<String> requestingParties, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionByEventIdRequest request =
        TransactionServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setEventId(eventId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransaction(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getFlatTransactionByEventId(request));
  }

  @Override
  public Single<Transaction> getFlatTransactionByEventId(
      String eventId, Set<String> requestingParties) {
    return getFlatTransactionByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<Transaction> getFlatTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getFlatTransactionByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<Transaction> getFlatTransactionById(
      String transactionId, Set<String> requestingParties, Optional<String> accessToken) {
    TransactionServiceOuterClass.GetTransactionByIdRequest request =
        TransactionServiceOuterClass.GetTransactionByIdRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setTransactionId(transactionId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransaction(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getFlatTransactionById(request));
  }

  @Override
  public Single<Transaction> getFlatTransactionById(
      String transactionId, Set<String> requestingParties) {
    return getFlatTransactionById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<Transaction> getFlatTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getFlatTransactionById(transactionId, requestingParties, Optional.of(accessToken));
  }

  private Single<LedgerOffset> getLedgerEnd(Optional<String> accessToken) {
    TransactionServiceOuterClass.GetLedgerEndRequest request =
        TransactionServiceOuterClass.GetLedgerEndRequest.newBuilder().setLedgerId(ledgerId).build();
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceFutureStub, accessToken).getLedgerEnd(request))
        .map(GetLedgerEndResponse::fromProto)
        .map(GetLedgerEndResponse::getOffset);
  }

  @Override
  public Single<LedgerOffset> getLedgerEnd() {
    return getLedgerEnd(Optional.empty());
  }

  @Override
  public Single<LedgerOffset> getLedgerEnd(String accessToken) {
    return getLedgerEnd(Optional.of(accessToken));
  }
}
