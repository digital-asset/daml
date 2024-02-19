// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v2.UpdateServiceGrpc;
import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.rxjava.UpdateClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.ClientPublisherFlowable;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

public final class UpdateClientImpl implements UpdateClient {
  private final UpdateServiceGrpc.UpdateServiceStub serviceStub;
  private final UpdateServiceGrpc.UpdateServiceFutureStub serviceFutureStub;
  private final ExecutionSequencerFactory sequencerFactory;

  public UpdateClientImpl(
      Channel channel, ExecutionSequencerFactory sequencerFactory, Optional<String> accessToken) {
    this.sequencerFactory = sequencerFactory;
    this.serviceStub = StubHelper.authenticating(UpdateServiceGrpc.newStub(channel), accessToken);
    this.serviceFutureStub =
        StubHelper.authenticating(UpdateServiceGrpc.newFutureStub(channel), accessToken);
  }

  private static <T> Iterable<T> toIterable(Optional<T> o) {
    return o.map(Collections::singleton).orElseGet(Collections::emptySet);
  }

  private Flowable<Transaction> extractTransactions(
      UpdateServiceOuterClass.GetUpdatesRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getUpdates,
            sequencerFactory)
        .map(GetUpdatesResponse::fromProto)
        .map(GetUpdatesResponse::getTransaction)
        .concatMapIterable(UpdateClientImpl::toIterable);
  }

  private Flowable<Transaction> getTransactions(
      ParticipantOffset begin,
      ParticipantOffset end,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequest(begin, end, filter, verbose).toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<Transaction> getTransactions(
      ParticipantOffset begin, ParticipantOffset end, TransactionFilter filter, boolean verbose) {
    return getTransactions(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<Transaction> getTransactions(
      ParticipantOffset begin,
      ParticipantOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken) {
    return getTransactions(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<Transaction> getTransactions(
      ParticipantOffset begin,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequest(
                begin, ParticipantOffset.ParticipantEnd.getInstance(), filter, verbose)
            .toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<Transaction> getTransactions(
      ParticipantOffset begin, TransactionFilter filter, boolean verbose) {
    return getTransactions(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<Transaction> getTransactions(
      ParticipantOffset begin, TransactionFilter filter, boolean verbose, String accessToken) {
    return getTransactions(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffset begin,
      Set<String> parties,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionFilter filter = contractFilter.transactionFilter(parties);
    return getTransactions(begin, filter, verbose, accessToken);
  }

  public Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffset begin,
      Set<String> parties,
      boolean verbose,
      String accessToken) {
    return getTransactions(contractFilter, begin, parties, verbose, Optional.of(accessToken));
  }

  public Flowable<Transaction> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffset begin,
      Set<String> parties,
      boolean verbose) {
    return getTransactions(contractFilter, begin, parties, verbose, Optional.empty());
  }

  private Flowable<TransactionTree> extractTransactionTrees(
      UpdateServiceOuterClass.GetUpdatesRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getUpdateTrees,
            sequencerFactory)
        .map(GetUpdateTreesResponse::fromProto)
        .map(GetUpdateTreesResponse::getTransactionTree)
        .concatMapIterable(UpdateClientImpl::toIterable);
  }

  private Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequest(
                begin, ParticipantOffset.ParticipantEnd.getInstance(), filter, verbose)
            .toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin, TransactionFilter filter, boolean verbose) {
    return getTransactionsTrees(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin, TransactionFilter filter, boolean verbose, String accessToken) {
    return getTransactionsTrees(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin,
      ParticipantOffset end,
      TransactionFilter filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequest(begin, end, filter, verbose).toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin, ParticipantOffset end, TransactionFilter filter, boolean verbose) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTree> getTransactionsTrees(
      ParticipantOffset begin,
      ParticipantOffset end,
      TransactionFilter filter,
      boolean verbose,
      String accessToken) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private Single<TransactionTree> extractTransactionTree(
      Future<UpdateServiceOuterClass.GetTransactionTreeResponse> future) {
    return Single.fromFuture(future)
        .map(GetTransactionTreeResponse::fromProto)
        .map(GetTransactionTreeResponse::getTransactionTree);
  }

  private Single<TransactionTree> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties, Optional<String> accessToken) {
    UpdateServiceOuterClass.GetTransactionByEventIdRequest request =
        UpdateServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
            .setEventId(eventId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransactionTree(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getTransactionTreeByEventId(request));
  }

  @Override
  public Single<TransactionTree> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties) {
    return getTransactionTreeByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTree> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getTransactionTreeByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<TransactionTree> getTransactionTreeById(
      String transactionId, Set<String> requestingParties, Optional<String> accessToken) {
    UpdateServiceOuterClass.GetTransactionByIdRequest request =
        UpdateServiceOuterClass.GetTransactionByIdRequest.newBuilder()
            .setUpdateId(transactionId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransactionTree(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getTransactionTreeById(request));
  }

  @Override
  public Single<TransactionTree> getTransactionTreeById(
      String transactionId, Set<String> requestingParties) {
    return getTransactionTreeById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTree> getTransactionTreeById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getTransactionTreeById(transactionId, requestingParties, Optional.of(accessToken));
  }

  private Single<Transaction> extractTransaction(
      Future<UpdateServiceOuterClass.GetTransactionResponse> future) {
    return Single.fromFuture(future)
        .map(GetTransactionResponse::fromProto)
        .map(GetTransactionResponse::getTransaction);
  }

  private Single<Transaction> getTransactionByEventId(
      String eventId, Set<String> requestingParties, Optional<String> accessToken) {
    UpdateServiceOuterClass.GetTransactionByEventIdRequest request =
        UpdateServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
            .setEventId(eventId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransaction(
        StubHelper.authenticating(this.serviceFutureStub, accessToken)
            .getTransactionByEventId(request));
  }

  @Override
  public Single<Transaction> getTransactionByEventId(
      String eventId, Set<String> requestingParties) {
    return getTransactionByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<Transaction> getTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getTransactionByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<Transaction> getTransactionById(
      String transactionId, Set<String> requestingParties, Optional<String> accessToken) {
    UpdateServiceOuterClass.GetTransactionByIdRequest request =
        UpdateServiceOuterClass.GetTransactionByIdRequest.newBuilder()
            .setUpdateId(transactionId)
            .addAllRequestingParties(requestingParties)
            .build();
    return extractTransaction(
        StubHelper.authenticating(this.serviceFutureStub, accessToken).getTransactionById(request));
  }

  @Override
  public Single<Transaction> getTransactionById(
      String transactionId, Set<String> requestingParties) {
    return getTransactionById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<Transaction> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getTransactionById(transactionId, requestingParties, Optional.of(accessToken));
  }
}
