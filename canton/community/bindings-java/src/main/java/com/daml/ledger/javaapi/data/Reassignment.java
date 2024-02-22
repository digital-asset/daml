// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TraceContextOuterClass;
import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class Reassignment implements WorkflowEvent {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final String offset;

  @NonNull private final Optional<UnassignedEvent> unassignedEvent;

  @NonNull private final Optional<AssignedEvent> assignedEvent;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  private Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull Optional<UnassignedEvent> unassignedEvent,
      @NonNull Optional<AssignedEvent> assignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.offset = offset;
    this.unassignedEvent = unassignedEvent;
    this.assignedEvent = assignedEvent;
    this.traceContext = traceContext;
  }

  public Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull UnassignedEvent unassignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this(
        updateId,
        commandId,
        workflowId,
        offset,
        Optional.of(unassignedEvent),
        Optional.empty(),
        traceContext);
  }

  public Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull AssignedEvent assignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this(
        updateId,
        commandId,
        workflowId,
        offset,
        Optional.empty(),
        Optional.of(assignedEvent),
        traceContext);
  }

  @NonNull
  public String getUpdateId() {
    return updateId;
  }

  @NonNull
  public String getCommandId() {
    return commandId;
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
  }

  @NonNull
  public String getOffset() {
    return offset;
  }

  @NonNull
  public Optional<UnassignedEvent> getUnassignedEvent() {
    return unassignedEvent;
  }

  @NonNull
  public Optional<AssignedEvent> getAssignedEvent() {
    return assignedEvent;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  public static Reassignment fromProto(ReassignmentOuterClass.Reassignment reassignment) {
    return new Reassignment(
        reassignment.getUpdateId(),
        reassignment.getCommandId(),
        reassignment.getWorkflowId(),
        reassignment.getOffset(),
        reassignment.hasUnassignedEvent()
            ? Optional.of(UnassignedEvent.fromProto(reassignment.getUnassignedEvent()))
            : Optional.empty(),
        reassignment.hasAssignedEvent()
            ? Optional.of(AssignedEvent.fromProto(reassignment.getAssignedEvent()))
            : Optional.empty(),
        reassignment.getTraceContext());
  }

  public ReassignmentOuterClass.Reassignment toProto() {
    var builder =
        ReassignmentOuterClass.Reassignment.newBuilder()
            .setUpdateId(updateId)
            .setCommandId(commandId)
            .setWorkflowId(workflowId)
            .setOffset(offset)
            .setTraceContext(traceContext);
    unassignedEvent.ifPresent(event -> builder.setUnassignedEvent(event.toProto()));
    assignedEvent.ifPresent(event -> builder.setAssignedEvent(event.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "Reassignment{"
        + "updateId='"
        + updateId
        + '\''
        + ", commandId='"
        + commandId
        + '\''
        + ", workflowId='"
        + workflowId
        + '\''
        + ", offset='"
        + offset
        + '\''
        + ", unassignedEvent="
        + unassignedEvent
        + ", assignedEvent="
        + assignedEvent
        + ", traceContext="
        + traceContext
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Reassignment that = (Reassignment) o;
    return Objects.equals(updateId, that.updateId)
        && Objects.equals(commandId, that.commandId)
        && Objects.equals(workflowId, that.workflowId)
        && Objects.equals(offset, that.offset)
        && Objects.equals(unassignedEvent, that.unassignedEvent)
        && Objects.equals(assignedEvent, that.assignedEvent)
        && Objects.equals(traceContext, that.traceContext);
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        updateId, commandId, workflowId, offset, unassignedEvent, assignedEvent, traceContext);
  }
}
