// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TraceContextOuterClass;
import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class ReassignmentV2 implements WorkflowEvent {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final String offset;

  @NonNull private final Optional<UnassignedEventV2> unassignedEvent;

  @NonNull private final Optional<AssignedEventV2> assignedEvent;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  private ReassignmentV2(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull Optional<UnassignedEventV2> unassignedEvent,
      @NonNull Optional<AssignedEventV2> assignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.offset = offset;
    this.unassignedEvent = unassignedEvent;
    this.assignedEvent = assignedEvent;
    this.traceContext = traceContext;
  }

  public ReassignmentV2(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull UnassignedEventV2 unassignedEvent,
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

  public ReassignmentV2(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull String offset,
      @NonNull AssignedEventV2 assignedEvent,
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
  public Optional<UnassignedEventV2> getUnassignedEvent() {
    return unassignedEvent;
  }

  @NonNull
  public Optional<AssignedEventV2> getAssignedEvent() {
    return assignedEvent;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  public static ReassignmentV2 fromProto(ReassignmentOuterClass.Reassignment reassignment) {
    return new ReassignmentV2(
        reassignment.getUpdateId(),
        reassignment.getCommandId(),
        reassignment.getWorkflowId(),
        reassignment.getOffset(),
        reassignment.hasUnassignedEvent()
            ? Optional.of(UnassignedEventV2.fromProto(reassignment.getUnassignedEvent()))
            : Optional.empty(),
        reassignment.hasAssignedEvent()
            ? Optional.of(AssignedEventV2.fromProto(reassignment.getAssignedEvent()))
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
    ReassignmentV2 that = (ReassignmentV2) o;
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
