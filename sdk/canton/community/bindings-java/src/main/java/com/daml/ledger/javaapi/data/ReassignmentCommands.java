// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;

/**
 * This class can be used to build a valid submission. It provides {@link #create(String, String, String, UnassignCommand)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withWorkflowId(String)}, {@link #withSubmissionId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = ReassignmentCommands.create(userId, commandId, synchronizerId, unassingnCommand)
 *                                   .withWorkflowId(workflowId)
 *                                   .with...
 * <pre/>
 */
public final class ReassignmentCommands {

  @NonNull private final Optional<String> workflowId;

  @NonNull private final String userId;
  @NonNull private final String commandId;
  @NonNull private final String submitter;
  @NonNull private final List<ReassignmentCommand> commands;

  @NonNull private final Optional<String> submissionId;

  protected ReassignmentCommands(
      @NonNull Optional<String> workflowId,
      @NonNull String userId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull List<ReassignmentCommand> commands,
      @NonNull Optional<String> submissionId) {
    this.workflowId = workflowId;
    this.userId = userId;
    this.commandId = commandId;
    this.submitter = submitter;
    this.commands = commands;
    this.submissionId = submissionId;
  }

  public static ReassignmentCommands create(
      @NonNull String userId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull UnassignCommand unassignCommand) {
    return new ReassignmentCommands(
        empty(), userId, commandId, submitter, Arrays.asList(unassignCommand), empty());
  }

  public static ReassignmentCommands create(
      @NonNull String userId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull AssignCommand assignCommand) {
    return new ReassignmentCommands(
        empty(), userId, commandId, submitter, Arrays.asList(assignCommand), empty());
  }

  public ReassignmentCommands withWorkflowId(@NonNull String workflowId) {
    return new ReassignmentCommands(
        Optional.of(workflowId), userId, commandId, submitter, commands, submissionId);
  }

  public ReassignmentCommands withSubmissionId(@NonNull String submissionId) {
    return new ReassignmentCommands(
        workflowId, userId, commandId, submitter, commands, Optional.of(submissionId));
  }

  @NonNull
  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  @NonNull
  public String getUserId() {
    return userId;
  }

  @NonNull
  public String getCommandId() {
    return commandId;
  }

  @NonNull
  public String getSubmitter() {
    return submitter;
  }

  @NonNull
  public List<ReassignmentCommand> getCommands() {
    return unmodifiableList(commands);
  }

  @NonNull
  public Optional<String> getSubmissionId() {
    return submissionId;
  }

  public ReassignmentCommandOuterClass.ReassignmentCommands toProto() {
    List<ReassignmentCommandOuterClass.ReassignmentCommand> commandsConverted =
        commands.stream().map(ReassignmentCommand::toProtoCommand).collect(Collectors.toList());

    var builder =
        ReassignmentCommandOuterClass.ReassignmentCommands.newBuilder()
            .setUserId(userId)
            .setCommandId(commandId)
            .setSubmitter(submitter)
            .addAllCommands(commandsConverted);

    workflowId.ifPresent(builder::setWorkflowId);
    submissionId.ifPresent(builder::setSubmissionId);

    return builder.build();
  }

  public static ReassignmentCommands fromProto(
      ReassignmentCommandOuterClass.ReassignmentCommands commands) {
    Optional<String> workflowId =
        commands.getWorkflowId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getWorkflowId());

    Optional<String> submissionId =
        commands.getSubmissionId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getSubmissionId());

    List<ReassignmentCommand> convertedCommands =
        commands.getCommandsList().stream()
            .map(ReassignmentCommand::fromProtoCommand)
            .collect(Collectors.toList());

    return new ReassignmentCommands(
        workflowId,
        commands.getUserId(),
        commands.getCommandId(),
        commands.getSubmitter(),
        convertedCommands,
        submissionId);
  }

  @Override
  public String toString() {
    return "ReassignmentCommands{"
        + "workflowId='"
        + workflowId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", submitter="
        + submitter
        + ", commands="
        + commands
        + ", submissionId='"
        + submissionId
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReassignmentCommands commandsSubmission = (ReassignmentCommands) o;
    return Objects.equals(workflowId, commandsSubmission.workflowId)
        && Objects.equals(userId, commandsSubmission.userId)
        && Objects.equals(commandId, commandsSubmission.commandId)
        && Objects.equals(submitter, commandsSubmission.submitter)
        && Objects.equals(commands, commandsSubmission.commands)
        && Objects.equals(submissionId, commandsSubmission.submissionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workflowId, userId, commandId, submitter, commands, submissionId);
  }
}
