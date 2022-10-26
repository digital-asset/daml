// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import static java.util.Arrays.asList;

import com.daml.ledger.api.v1.CommandServiceGrpc;
import com.daml.ledger.api.v1.CommandServiceOuterClass;
import com.daml.ledger.javaapi.data.*;
import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Exercised;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.rxjava.CommandClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CommandClientImpl implements CommandClient {

  private final String ledgerId;
  private final CommandServiceGrpc.CommandServiceFutureStub serviceStub;

  public CommandClientImpl(
      @NonNull String ledgerId, @NonNull Channel channel, @NonNull Optional<String> accessToken) {
    this.ledgerId = ledgerId;
    this.serviceStub =
        StubHelper.authenticating(CommandServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull Optional<String> accessToken) {
    CommandServiceOuterClass.SubmitAndWaitRequest request =
        SubmitAndWaitRequest.toProto(
            this.ledgerId,
            workflowId,
            applicationId,
            commandId,
            actAs,
            readAs,
            minLedgerTimeAbs,
            minLedgerTimeRel,
            deduplicationTime,
            toCommands(commands));
    return Single.fromFuture(
        StubHelper.authenticating(this.serviceStub, accessToken).submitAndWait(request));
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWait(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  private Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull Optional<String> accessToken) {
    CommandServiceOuterClass.SubmitAndWaitRequest request =
        SubmitAndWaitRequest.toProto(
            this.ledgerId,
            workflowId,
            applicationId,
            commandId,
            actAs,
            readAs,
            minLedgerTimeAbs,
            minLedgerTimeRel,
            deduplicationTime,
            toCommands(commands));
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .submitAndWaitForTransactionId(request))
        .map(CommandServiceOuterClass.SubmitAndWaitForTransactionIdResponse::getTransactionId);
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionId(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  private Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull Optional<String> accessToken) {
    CommandServiceOuterClass.SubmitAndWaitRequest request =
        SubmitAndWaitRequest.toProto(
            this.ledgerId,
            workflowId,
            applicationId,
            commandId,
            actAs,
            readAs,
            minLedgerTimeAbs,
            minLedgerTimeRel,
            deduplicationTime,
            toCommands(commands));
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .submitAndWaitForTransaction(request))
        .map(CommandServiceOuterClass.SubmitAndWaitForTransactionResponse::getTransaction)
        .map(Transaction::fromProto);
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransaction(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  private Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull Optional<String> accessToken) {
    CommandServiceOuterClass.SubmitAndWaitRequest request =
        SubmitAndWaitRequest.toProto(
            this.ledgerId,
            workflowId,
            applicationId,
            commandId,
            actAs,
            readAs,
            minLedgerTimeAbs,
            minLedgerTimeRel,
            deduplicationTime,
            toCommands(commands));
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .submitAndWaitForTransactionTree(request))
        .map(CommandServiceOuterClass.SubmitAndWaitForTransactionTreeResponse::getTransaction)
        .map(TransactionTree::fromProto);
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken) {
    return submitAndWaitForTransactionTree(
        workflowId,
        applicationId,
        commandId,
        actAs,
        readAs,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  private <T> CreatedEvent singleCreatedEvent(List<T> events) {
    if (events.size() == 1 && events.get(0) instanceof CreatedEvent)
      return (CreatedEvent) events.get(0);
    throw new IllegalArgumentException(
        "Expected exactly one created event from the transaction, got: " + events);
  }

  private ExercisedEvent firstExercisedEvent(TransactionTree txTree) {
    var maybeExercisedEvent =
        txTree.getRootEventIds().stream()
            .map(eventId -> txTree.getEventsById().get(eventId))
            .filter(e -> e instanceof ExercisedEvent)
            .map(e -> (ExercisedEvent) e)
            .findFirst();

    return maybeExercisedEvent.orElseThrow(
        () ->
            new IllegalArgumentException("Expect an exercised event but not found. tx: " + txTree));
  }

  private <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update,
      @NonNull Optional<String> accessToken) {
    return update.foldUpdate(
        new Update.FoldUpdate<>() {
          @Override
          public <CtId> Single<U> created(Update.CreateUpdate<CtId, U> create) {
            var transaction =
                submitAndWaitForTransaction(
                    workflowId,
                    applicationId,
                    commandId,
                    actAs,
                    readAs,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    update.commands(),
                    accessToken);
            return transaction.map(
                tx -> {
                  var createdEvent = singleCreatedEvent(tx.getEvents());
                  return create.k.apply(Created.fromEvent(create.createdContractId, createdEvent));
                });
          }

          @Override
          public <R> Single<U> exercised(Update.ExerciseUpdate<R, U> exercise) {
            var transactionTree =
                submitAndWaitForTransactionTree(
                    workflowId,
                    applicationId,
                    commandId,
                    actAs,
                    readAs,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    update.commands(),
                    accessToken);
            return transactionTree.map(
                txTree -> {
                  var exercisedEvent = firstExercisedEvent(txTree);
                  return exercise.k.apply(
                      Exercised.fromEvent(exercise.returnTypeDecoder, exercisedEvent));
                });
          }
        });
  }

  @Override
  public <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update) {
    return submitAndWaitForResult(
        workflowId, applicationId, commandId, actAs, readAs, update, Optional.empty());
  }

  @Override
  public <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update,
      @NonNull String accessToken) {
    return submitAndWaitForResult(
        workflowId, applicationId, commandId, actAs, readAs, update, Optional.of(accessToken));
  }

  static List<Command> toCommands(@NonNull List<@NonNull ? extends HasCommands> hasCommands) {
    return hasCommands.stream().flatMap(c -> c.commands().stream()).collect(Collectors.toList());
  }
}
