// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandServiceOuterClass;
import com.daml.ledger.rxjava.grpc.CommandsSubmission;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class SubmitAndWaitRequest {

  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId, @NonNull CommandsSubmission commandsSubmission) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                commandsSubmission.getWorkflowId(),
                commandsSubmission.getApplicationId(),
                commandsSubmission.getCommandId(),
                commandsSubmission.getActAs(),
                commandsSubmission.getReadAs(),
                commandsSubmission.getMinLedgerTimeAbs(),
                commandsSubmission.getMinLedgerTimeRel(),
                commandsSubmission.getDeduplicationTime(),
                (List<Command>) commandsSubmission.getCommands()) // TODO: remove this cast
            )
        .build();
  }

  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String submissionId,
      @NonNull CommandsSubmission commandsSubmission) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                commandsSubmission.getWorkflowId(),
                commandsSubmission.getApplicationId(),
                commandsSubmission.getCommandId(),
                submissionId,
                commandsSubmission.getActAs(),
                commandsSubmission.getReadAs(),
                commandsSubmission.getMinLedgerTimeAbs(),
                commandsSubmission.getMinLedgerTimeRel(),
                commandsSubmission.getDeduplicationTime(),
                (List<Command>) commandsSubmission.getCommands()) // TODO: remove this cast
            )
        .build();
  }

  /**
   * Please use {@link #toProto(String, CommandsSubmission)}
   *
   * @deprecated
   * @since 2.5
   */
  @Deprecated
  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                party,
                minLedgerTimeAbsolute,
                minLedgerTimeRelative,
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
  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull String party,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                submissionId,
                party,
                minLedgerTimeAbsolute,
                minLedgerTimeRelative,
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
  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                actAs,
                readAs,
                minLedgerTimeAbsolute,
                minLedgerTimeRelative,
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
  public static CommandServiceOuterClass.SubmitAndWaitRequest toProto(
      @NonNull String ledgerId,
      @NonNull String workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submissionId,
      @NonNull List<@NonNull String> actAs,
      @NonNull List<@NonNull String> readAs,
      @NonNull Optional<Instant> minLedgerTimeAbsolute,
      @NonNull Optional<Duration> minLedgerTimeRelative,
      @NonNull Optional<Duration> deduplicationTime,
      @NonNull List<@NonNull Command> commands) {
    return CommandServiceOuterClass.SubmitAndWaitRequest.newBuilder()
        .setCommands(
            SubmitCommandsRequest.toProto(
                ledgerId,
                workflowId,
                applicationId,
                commandId,
                submissionId,
                actAs,
                readAs,
                minLedgerTimeAbsolute,
                minLedgerTimeRelative,
                deduplicationTime,
                commands))
        .build();
  }
}
