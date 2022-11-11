// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.rxjava.grpc.CommandClientConfig;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandServiceGrpc} */
public interface CommandClient {
  // TODO: Deprecate old methods and javadoc new ones
  Single<Empty> submitAndWait(CommandClientConfig params);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * TODO: add deprecation doc and annotations This method has been deprecated as of [???]. Please
   * use {@link #submitAndWait(CommandClientConfig)} instead
   */
  //  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(CommandClientConfig params);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Transaction> submitAndWaitForTransaction(CommandClientConfig params);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<TransactionTree> submitAndWaitForTransactionTree(CommandClientConfig params);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  <U> Single<U> submitAndWaitForResult(CommandClientConfig params, @NonNull Update<U> update);

  <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update);

  <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update,
      @NonNull String accessToken);
}
