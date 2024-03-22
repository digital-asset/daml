// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmission;
import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
import com.daml.ledger.javaapi.data.UpdateSubmission;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandServiceGrpc} */
public interface CommandClient {
  Single<Empty> submitAndWait(CommandsSubmission submission);

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
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

  /** @deprecated since 2.5. Use {@link #submitAndWait(CommandsSubmission)} instead. */
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

  /** @deprecated since 2.5. Use {@link #submitAndWait(CommandsSubmission)} instead. */
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

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
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

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /** @deprecated since 2.5. Please use {@link #submitAndWait(CommandsSubmission)} instead */
  @Deprecated
  Single<Empty> submitAndWait(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  Single<String> submitAndWaitForTransactionId(CommandsSubmission submission);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     (CommandsSubmission)} instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
   */
  @Deprecated
  Single<String> submitAndWaitForTransactionId(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionId(CommandsSubmission)}
   *     instead
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

  Single<Transaction> submitAndWaitForTransaction(CommandsSubmission submission);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
   */
  @Deprecated
  Single<Transaction> submitAndWaitForTransaction(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransaction(CommandsSubmission)}
   *     instead
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

  Single<TransactionTree> submitAndWaitForTransactionTree(CommandsSubmission submission);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
   */
  @Deprecated
  Single<TransactionTree> submitAndWaitForTransactionTree(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForTransactionTree(CommandsSubmission)}
   *     instead
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

  /** @deprecated since 2.5. Please use {@link #submitAndWaitForResult(UpdateSubmission)} instead */
  @Deprecated
  <U> Single<U> submitAndWaitForResult(CommandsSubmission submission, @NonNull Update<U> update);

  <U> Single<U> submitAndWaitForResult(@NonNull UpdateSubmission<U> submission);

  /**
   * @deprecated since 2.5. Please use {@link #submitAndWaitForResult(CommandsSubmission, Update)}
   *     instead
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
   * @deprecated since 2.5. Please use {@link #submitAndWaitForResult(CommandsSubmission, Update)}
   *     instead
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
