// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.daml.grpc.adapter.ExecutionSequencerFactory;
import com.daml.ledger.api.v1.CommandSubmissionServiceGrpc;
import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;
import com.daml.ledger.javaapi.data.Command;
import com.daml.ledger.javaapi.data.SubmitRequest;
import com.daml.ledger.rxjava.CommandSubmissionClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.rxjava.util.CreateSingle;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public class CommandSubmissionClientImpl implements CommandSubmissionClient {

  private final String ledgerId;
  private final CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub serviceStub;
  private final ExecutionSequencerFactory sequencerFactory;
  private final Optional<Duration> timeout;

  public CommandSubmissionClientImpl(
      @NonNull String ledgerId,
      @NonNull Channel channel,
      ExecutionSequencerFactory sequencerFactory,
      Optional<String> accessToken,
      Optional<Duration> timeout) {
    this.ledgerId = ledgerId;
    this.timeout = timeout;
    this.sequencerFactory = sequencerFactory;
    this.serviceStub =
        StubHelper.authenticating(CommandSubmissionServiceGrpc.newFutureStub(channel), accessToken);
  }

  public Single<com.google.protobuf.Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands,
      Optional<String> accessToken) {
    CommandSubmissionServiceOuterClass.SubmitRequest request =
        SubmitRequest.toProto(
            ledgerId,
            workflowId,
            applicationId,
            commandId,
            actAs,
            readAs,
            minLedgerTimeAbs,
            minLedgerTimeRel,
            deduplicationTime,
            commands);
    CommandSubmissionServiceGrpc.CommandSubmissionServiceFutureStub stubWithTimeout =
        this.timeout
            .map(t -> this.serviceStub.withDeadlineAfter(t.toMillis(), MILLISECONDS))
            .orElse(this.serviceStub);
    return CreateSingle.fromFuture(
        StubHelper.authenticating(stubWithTimeout, accessToken).submit(request), sequencerFactory);
  }

  public Single<com.google.protobuf.Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands,
      Optional<String> accessToken) {
    return this.submit(
        workflowId,
        applicationId,
        commandId,
        asList(party),
        asList(),
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        accessToken);
  }

  @Override
  public Single<com.google.protobuf.Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull Command> commands) {
    return submit(
        workflowId,
        applicationId,
        commandId,
        party,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull Command> commands) {
    return submit(
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
  public Single<com.google.protobuf.Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands,
      @NonNull String accessToken) {
    return submit(
        workflowId,
        applicationId,
        commandId,
        party,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands,
      @NonNull String accessToken) {
    return submit(
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
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return submit(
        workflowId,
        applicationId,
        commandId,
        party,
        minLedgerTimeAbs,
        minLedgerTimeRel,
        deduplicationTime,
        commands,
        Optional.empty());
  }

  @Override
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return submit(
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
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull Command> commands,
      @NonNull String accessToken) {
    return submit(
        workflowId,
        applicationId,
        commandId,
        party,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        commands,
        Optional.of(accessToken));
  }

  @Override
  public Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull Command> commands,
      @NonNull String accessToken) {
    return submit(
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
}
