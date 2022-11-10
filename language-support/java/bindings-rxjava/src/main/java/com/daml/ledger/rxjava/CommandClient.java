// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.Transaction;
import com.daml.ledger.javaapi.data.TransactionTree;
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

  //TODO: Shouldn't these be deprecated and not removed?
  Single<Empty> submitAndWait(
          @NonNull String workflowId,
          @NonNull String applicationId,
          @NonNull String commandId,
          @NonNull List<@NonNull String> actAs, // party
          @NonNull List<@NonNull String> readAs,
          @NonNull Optional<Instant> minLedgerTimeAbs,
          @NonNull Optional<Duration> minLedgerTimeRel,
          @NonNull Optional<Duration> deduplicationTime,
          @NonNull List<@NonNull ? extends HasCommands> commands,
          @NonNull Optional<String> accessToken);

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
      @NonNull Optional<String> accessToken);

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
          @NonNull Optional<String> accessToken);

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
          @NonNull Optional<String> accessToken);

  <U> Single<U> submitAndWaitForResult(
          @NonNull String workflowId,
          @NonNull String applicationId,
          @NonNull String commandId,
          @NonNull List<@NonNull String> actAs,
          @NonNull List<@NonNull String> readAs,
          @NonNull Update<U> update,
          @NonNull Optional<String> accessToken);
}
