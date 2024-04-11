// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentCommandOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.empty;

/**
 * This class can be used to build a valid submission. It provides {@link #create(String, String, String, UnassignCommand)}
 * for initial creation and methods to set optional parameters
 * e.g {@link #withWorkflowId(String)}, {@link #withSubmissionId(String)} etc.
 *
 * Usage:
 * <pre>
 *   var submission = ReassignmentCommand.create(applicationId, commandId, domainId, unassingnCommand)
 *                                   .withWorkflowId(workflowId)
 *                                   .with...
 * <pre/>
 */
public final class ReassignmentCommand {

  @NonNull private final Optional<String> workflowId;

  @NonNull private final String applicationId;
  @NonNull private final String commandId;
  @NonNull private final String submitter;
  @NonNull private final Optional<UnassignCommand> unassignCommand;
  @NonNull private final Optional<AssignCommand> assignCommand;

  @NonNull private final Optional<String> submissionId;

  protected ReassignmentCommand(
      @NonNull Optional<String> workflowId,
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull Optional<UnassignCommand> unassignCommand,
      @NonNull Optional<AssignCommand> assignCommand,
      @NonNull Optional<String> submissionId) {
    this.workflowId = workflowId;
    this.applicationId = applicationId;
    this.commandId = commandId;
    this.submitter = submitter;
    this.unassignCommand = unassignCommand;
    this.assignCommand = assignCommand;
    this.submissionId = submissionId;
  }

  public static ReassignmentCommand create(
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull UnassignCommand unassignCommand) {
    return new ReassignmentCommand(
        empty(),
        applicationId,
        commandId,
        submitter,
        Optional.of(unassignCommand),
        empty(),
        empty());
  }

  public static ReassignmentCommand create(
      @NonNull String applicationId,
      @NonNull String commandId,
      @NonNull String submitter,
      @NonNull AssignCommand assignCommand) {
    return new ReassignmentCommand(
        empty(), applicationId, commandId, submitter, empty(), Optional.of(assignCommand), empty());
  }

  public ReassignmentCommand withWorkflowId(@NonNull String workflowId) {
    return new ReassignmentCommand(
        Optional.of(workflowId),
        applicationId,
        commandId,
        submitter,
        unassignCommand,
        assignCommand,
        submissionId);
  }

  public ReassignmentCommand withSubmissionId(@NonNull String submissionId) {
    return new ReassignmentCommand(
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
  public Optional<UnassignCommand> getUnassignCommand() {
    return unassignCommand;
  }

  @NonNull
  public Optional<AssignCommand> getAssignCommand() {
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

  public static ReassignmentCommand fromProto(
      ReassignmentCommandOuterClass.ReassignmentCommand commands) {
    Optional<String> workflowId =
        commands.getWorkflowId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getWorkflowId());

    Optional<String> submissionId =
        commands.getSubmissionId().isEmpty()
            ? Optional.empty()
            : Optional.of(commands.getSubmissionId());

    Optional<UnassignCommand> unassignCommand =
        commands.hasUnassignCommand()
            ? Optional.of(UnassignCommand.fromProto(commands.getUnassignCommand()))
            : Optional.empty();

    Optional<AssignCommand> assignCommand =
        commands.hasAssignCommand()
            ? Optional.of(AssignCommand.fromProto(commands.getAssignCommand()))
            : Optional.empty();

    return new ReassignmentCommand(
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
    ReassignmentCommand commandsSubmission = (ReassignmentCommand) o;
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
