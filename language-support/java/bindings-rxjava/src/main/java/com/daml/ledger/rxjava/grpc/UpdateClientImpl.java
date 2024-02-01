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

  private Flowable<TransactionV2> extractTransactions(
      UpdateServiceOuterClass.GetUpdatesRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getUpdates,
            sequencerFactory)
        .map(GetUpdatesResponseV2::fromProto)
        .map(GetUpdatesResponseV2::getTransaction)
        .concatMapIterable(UpdateClientImpl::toIterable);
  }

  private Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequestV2(begin, end, filter, verbose).toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose) {
    return getTransactions(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      String accessToken) {
    return getTransactions(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin,
      TransactionFilterV2 filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequestV2(
                begin, ParticipantOffsetV2.ParticipantEnd.getInstance(), filter, verbose)
            .toProto();
    return extractTransactions(request, accessToken);
  }

  @Override
  public Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose) {
    return getTransactions(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionV2> getTransactions(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose, String accessToken) {
    return getTransactions(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionV2> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffsetV2 begin,
      Set<String> parties,
      boolean verbose,
      Optional<String> accessToken) {
    TransactionFilterV2 filter = contractFilter.transactionFilter(parties);
    return getTransactions(begin, filter, verbose, accessToken);
  }

  public Flowable<TransactionV2> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffsetV2 begin,
      Set<String> parties,
      boolean verbose,
      String accessToken) {
    return getTransactions(contractFilter, begin, parties, verbose, Optional.of(accessToken));
  }

  public Flowable<TransactionV2> getTransactions(
      ContractFilter<?> contractFilter,
      ParticipantOffsetV2 begin,
      Set<String> parties,
      boolean verbose) {
    return getTransactions(contractFilter, begin, parties, verbose, Optional.empty());
  }

  private Flowable<TransactionTreeV2> extractTransactionTrees(
      UpdateServiceOuterClass.GetUpdatesRequest request, Optional<String> accessToken) {
    return ClientPublisherFlowable.create(
            request,
            StubHelper.authenticating(this.serviceStub, accessToken)::getUpdateTrees,
            sequencerFactory)
        .map(GetUpdateTreesResponseV2::fromProto)
        .map(GetUpdateTreesResponseV2::getTransactionTree)
        .concatMapIterable(UpdateClientImpl::toIterable);
  }

  private Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      TransactionFilterV2 filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequestV2(
                begin, ParticipantOffsetV2.ParticipantEnd.getInstance(), filter, verbose)
            .toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose) {
    return getTransactionsTrees(begin, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin, TransactionFilterV2 filter, boolean verbose, String accessToken) {
    return getTransactionsTrees(begin, filter, verbose, Optional.of(accessToken));
  }

  private Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      Optional<String> accessToken) {
    UpdateServiceOuterClass.GetUpdatesRequest request =
        new GetUpdatesRequestV2(begin, end, filter, verbose).toProto();
    return extractTransactionTrees(request, accessToken);
  }

  @Override
  public Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.empty());
  }

  @Override
  public Flowable<TransactionTreeV2> getTransactionsTrees(
      ParticipantOffsetV2 begin,
      ParticipantOffsetV2 end,
      TransactionFilterV2 filter,
      boolean verbose,
      String accessToken) {
    return getTransactionsTrees(begin, end, filter, verbose, Optional.of(accessToken));
  }

  private Single<TransactionTreeV2> extractTransactionTree(
      Future<UpdateServiceOuterClass.GetTransactionTreeResponse> future) {
    return Single.fromFuture(future)
        .map(GetTransactionTreeResponseV2::fromProto)
        .map(GetTransactionTreeResponseV2::getTransactionTree);
  }

  private Single<TransactionTreeV2> getTransactionTreeByEventId(
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
  public Single<TransactionTreeV2> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties) {
    return getTransactionTreeByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTreeV2> getTransactionTreeByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getTransactionTreeByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<TransactionTreeV2> getTransactionTreeById(
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
  public Single<TransactionTreeV2> getTransactionTreeById(
      String transactionId, Set<String> requestingParties) {
    return getTransactionTreeById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionTreeV2> getTransactionTreeById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getTransactionTreeById(transactionId, requestingParties, Optional.of(accessToken));
  }

  private Single<TransactionV2> extractTransaction(
      Future<UpdateServiceOuterClass.GetTransactionResponse> future) {
    return Single.fromFuture(future)
        .map(GetTransactionResponseV2::fromProto)
        .map(GetTransactionResponseV2::getTransaction);
  }

  private Single<TransactionV2> getTransactionByEventId(
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
  public Single<TransactionV2> getTransactionByEventId(
      String eventId, Set<String> requestingParties) {
    return getTransactionByEventId(eventId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionV2> getTransactionByEventId(
      String eventId, Set<String> requestingParties, String accessToken) {
    return getTransactionByEventId(eventId, requestingParties, Optional.of(accessToken));
  }

  private Single<TransactionV2> getTransactionById(
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
  public Single<TransactionV2> getTransactionById(
      String transactionId, Set<String> requestingParties) {
    return getTransactionById(transactionId, requestingParties, Optional.empty());
  }

  @Override
  public Single<TransactionV2> getTransactionById(
      String transactionId, Set<String> requestingParties, String accessToken) {
    return getTransactionById(transactionId, requestingParties, Optional.of(accessToken));
  }
}
