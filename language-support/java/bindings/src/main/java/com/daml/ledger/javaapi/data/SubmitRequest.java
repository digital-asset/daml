// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandSubmissionServiceOuterClass;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitRequest {

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId, @NonNull CommandsSubmission commandsSubmission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(SubmitCommandsRequest.toProto(ledgerId, commandsSubmission))
        .build();
  }

  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String submissionId,
      @NonNull CommandsSubmission commandsSubmission) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(SubmitCommandsRequest.toProto(ledgerId, submissionId, commandsSubmission))
        .build();
  }

  /**
   * Please use {@link #toProto(String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                party,
                minLedgerTimeAbs,
                minLedgerTimeRel,
                deduplicationTime,
                commands))
        .build();
  }

  /**
   * Please use {@link #toProto(String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                actAs,
                readAs,
                minLedgerTimeAbs,
                minLedgerTimeRel,
                deduplicationTime,
                commands))
        .build();
  }

  /**
   * Please use {@link #toProto(String, String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                submissionId,
                party,
                minLedgerTimeAbs,
                minLedgerTimeRel,
                deduplicationTime,
                commands))
        .build();
  }

  /**
   * Please use {@link #toProto(String, String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandSubmissionServiceOuterClass.SubmitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbs,
      @NonNull Optional<Duration> minLedgerTimeRel,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandSubmissionServiceOuterClass.SubmitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                submissionId,
                actAs,
                readAs,
                minLedgerTimeAbs,
                minLedgerTimeRel,
                deduplicationTime,
                commands))
        .build();
  }
}
