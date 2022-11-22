// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.CommandsSubmission;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import com.google.protobuf.Empty;
import io.reactivex.Single;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

/** An RxJava version of {@link com.daml.ledger.api.v1.CommandSubmissionServiceGrpc} */
public interface CommandSubmissionClient {

  Single<Empty> submit(CommandsSubmission submission);

  /**
   * Use {@link #submit(CommandsSubmission)} instead
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
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
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
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
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
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
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);

  /**
   * This method has been deprecated as of [???]. Please use {@link #submit(CommandsSubmission)}
   * instead
   *
   * @deprecated
   */
  @Deprecated
  Single<Empty> submit(
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull List<@NonNull ? extends HasCommands> commands,
      @NonNull String accessToken);
}
