// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import com.daml.ledger.javaapi.data.codegen.HasCommands;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Optional.empty;

/**
 * This class can be used to build a valid submission. It provides {@link #create(String, String, String, UnassignCommandV2)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withWorkflowId(String)}, {@link #withSubmissionId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = ReassignmentCommandV2.create(applicationId, commandId, domainId, unassingnCommand)
 *                                   .withWorkflowId(workflowId)
 *                                   .with...
 * <pre/>
 */
public final class ReassignmentCommandV2 {

  @NonNull private final Optional<String> workflowId;

  @NonNull private final String applicationId;
  @NonNull private final String commandId;
  @NonNull private final String submitter;
  @NonNull private final Optional<UnassignCommandV2> unassignCommand;
  @NonNull private final Optional<AssignCommandV2> assignCommand;

  @NonNull private final Optional<String> submissionId;

  protected ReassignmentCommandV2(
      @NonNull Optional<String> workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull Optional<UnassignCommandV2> unassignCommand,
      @NonNull Optional<AssignCommandV2> assignCommand,
      @NonNull Optional<String> submissionId) {
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.submitter = submitter;
    this.unassignCommand = unassignCommand;
    this.assignCommand = assignCommand;
    this.submissionId = submissionId;
  }

  public static ReassignmentCommandV2 create(
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull UnassignCommandV2 unassignCommand) {
    return new ReassignmentCommandV2(
        empty(),
        applicationId,
        commandId,
        submitter,
        Optional.of(unassignCommand),
        empty(),
        empty());
  }

  public static ReassignmentCommandV2 create(
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull AssignCommandV2 assignCommand) {
    return new ReassignmentCommandV2(
        empty(), applicationId, commandId, submitter, empty(), Optional.of(assignCommand), empty());
  }

  public ReassignmentCommandV2 withWorkflowId(@NonNull String workflowId) {
    return new ReassignmentCommandV2(
        Optional.of(workflowId),
        applicationId,
        commandId,
        submitter,
        unassignCommand,
        assignCommand,
        submissionId);
  }

  public ReassignmentCommandV2 withSubmissionId(@NonNull String submissionId) {
    return new ReassignmentCommandV2(
        workflowId,
        applicationId,
        commandId,
        submitter,
        unassignCommand,
        assignCommand,
        Optional.of(submissionId));
  }

  @NonNull
  public Optional<String> getWorkflowId() {
    return workflowId;
  }

  @NonNull
  public String getApplicationId() {
    return applicationId;
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
  public Optional<UnassignCommandV2> getUnassignCommand() {
    return unassignCommand;
  }

  @NonNull
  public Optional<AssignCommandV2> getAssignCommand() {
    return assignCommand;
  }

  @NonNull
  public Optional<String> getSubmissionId() {
    return submissionId;
  }

  public ReassignmentCommandOuterClass.ReassignmentCommand toProto() {

    var builder =
        ReassignmentCommandOuterClass.ReassignmentCommand.newBuilder()
            .setApplicationId(applicationId)
            .setCommandId(commandId)
            .setSubmitter(submitter);

    workflowId.ifPresent(builder::setWorkflowId);
    submissionId.ifPresent(builder::setSubmissionId);
    unassignCommand.ifPresent(unassign -> builder.setUnassignCommand(unassign.toProto()));
    assignCommand.ifPresent(assign -> builder.setAssignCommand(assign.toProto()));

    return builder.build();
  }

  public static ReassignmentCommandV2 fromProto(
      ReassignmentCommandOuterClass.ReassignmentCommand commands) {
    Optional<String> workflowId =
        commands.getWorkflowId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getWorkflowId());

    Optional<String> submissionId =
        commands.getSubmissionId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getSubmissionId());

    Optional<UnassignCommandV2> unassignCommand =
        commands.hasUnassignCommand()
            ? Optional.of(UnassignCommandV2.fromProto(commands.getUnassignCommand()))
            : Optional.empty();

    Optional<AssignCommandV2> assignCommand =
        commands.hasAssignCommand()
            ? Optional.of(AssignCommandV2.fromProto(commands.getAssignCommand()))
            : Optional.empty();

    return new ReassignmentCommandV2(
        workflowId,
        commands.getApplicationId(),
        commands.getCommandId(),
        commands.getSubmitter(),
        unassignCommand,
        assignCommand,
        submissionId);
  }

  @Override
  public String toString() {
    return "ReassignmentCommand{"
        + "workflowId='"
        + workflowId
        + '\''
        + ", applicationId='"
        + applicationId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", submitter="
        + submitter
        + ", unassignCommand="
        + unassignCommand
        + ", assignCommand="
        + assignCommand
        + ", submissionId='"
        + submissionId
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReassignmentCommandV2 commandsSubmission = (ReassignmentCommandV2) o;
    return Objects.equals(workflowId, commandsSubmission.workflowId)
        && Objects.equals(applicationId, commandsSubmission.applicationId)
        && Objects.equals(commandId, commandsSubmission.commandId)
        && Objects.equals(submitter, commandsSubmission.submitter)
        && Objects.equals(unassignCommand, commandsSubmission.unassignCommand)
        && Objects.equals(assignCommand, commandsSubmission.assignCommand)
        && Objects.equals(submissionId, commandsSubmission.submissionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        workflowId,
        applicationId,
        commandId,
        submitter,
        unassignCommand,
        assignCommand,
        submissionId);
  }
}
