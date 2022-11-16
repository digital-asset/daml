// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.rxjava.grpc.CommandsBuilder;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandServiceGrpc} */
public interface CommandClient {
  Single<Empty> submitAndWait(CommandsBuilder params);

  /**
   * Use {@link #submitAndWait(CommandsBuilder)} instead
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
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
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submitAndWait(CommandsBuilder)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(CommandsBuilder params);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionId(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<Transaction> submitAndWaitForTransaction(CommandsBuilder params);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransaction(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<TransactionTree> submitAndWaitForTransactionTree(CommandsBuilder params);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
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

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForTransactionTree(CommandsBuilder)} instead
   *
   * @deprecated
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  <U> Single<U> submitAndWaitForResult(CommandsBuilder params, @NonNull Update<U> update);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForResult(CommandsBuilder, Update)} instead
   *
   * @deprecated
   */
  @Deprecated
  <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update);

  /**
   * This method has been deprecated as of [???]. Please use {@link
   * #submitAndWaitForResult(CommandsBuilder, Update)} instead
   *
   * @deprecated
   */
  @Deprecated
  <U> Single<U> submitAndWaitForResult(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Update<U> update,
      @NonNull String accessToken);
}
