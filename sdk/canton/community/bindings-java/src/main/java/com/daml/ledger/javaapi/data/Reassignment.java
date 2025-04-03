// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TraceContextOuterClass;
import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class Reassignment implements WorkflowEvent {

  @NonNull private final String updateId;

  @NonNull private final String commandId;

  @NonNull private final String workflowId;

  @NonNull private final Long offset;

  @NonNull private final List<ReassignmentEvent> events;

  private final TraceContextOuterClass.@NonNull TraceContext traceContext;

  @NonNull private final Instant recordTime;

  private Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Long offset,
      @NonNull List<ReassignmentEvent> events,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this.updateId = updateId;
    this.commandId = commandId;
    this.workflowId = workflowId;
    this.offset = offset;
    this.events = events;
    this.traceContext = traceContext;
    this.recordTime = recordTime;
  }

  public Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Long offset,
      @NonNull UnassignedEvent unassignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this(
        updateId,
        commandId,
        workflowId,
        offset,
        Arrays.asList(unassignedEvent),
        traceContext,
        recordTime);
  }

  public Reassignment(
      @NonNull String updateId,
      @NonNull String commandId,
      @NonNull String workflowId,
      @NonNull Long offset,
      @NonNull AssignedEvent assignedEvent,
      TraceContextOuterClass.@NonNull TraceContext traceContext,
      @NonNull Instant recordTime) {
    this(
        updateId,
        commandId,
        workflowId,
        offset,
        Arrays.asList(assignedEvent),
        traceContext,
        recordTime);
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
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public List<ReassignmentEvent> getEvents() {
    return events;
  }

  public TraceContextOuterClass.@NonNull TraceContext getTraceContext() {
    return traceContext;
  }

  @NonNull
  public Instant getRecordTime() {
    return recordTime;
  }

  public static Reassignment fromProto(ReassignmentOuterClass.Reassignment reassignment) {
    List<ReassignmentEvent> events =
        reassignment.getEventsList().stream()
            .map(ReassignmentEvent::fromProtoEvent)
            .collect(Collectors.toList());
    return new Reassignment(
        reassignment.getUpdateId(),
        reassignment.getCommandId(),
        reassignment.getWorkflowId(),
        reassignment.getOffset(),
        events,
        reassignment.getTraceContext(),
        Utils.instantFromProto(reassignment.getRecordTime()));
  }

  public ReassignmentOuterClass.Reassignment toProto() {
    return ReassignmentOuterClass.Reassignment.newBuilder()
        .setUpdateId(updateId)
        .setCommandId(commandId)
        .setWorkflowId(workflowId)
        .setOffset(offset)
        .addAllEvents(
            events.stream().map(ReassignmentEvent::toProtoEvent).collect(Collectors.toList()))
        .setTraceContext(traceContext)
        .setRecordTime(Utils.instantToProto(recordTime))
        .build();
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
        + ", events="
        + events
        + ", traceContext="
        + traceContext
        + ", recordTime="
        + recordTime
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
        && Objects.equals(events, that.events)
        && Objects.equals(traceContext, that.traceContext)
        && Objects.equals(recordTime, that.recordTime);
  }

  @Override
  public int hashCode() {

    return Objects.hash(updateId, commandId, workflowId, offset, events, traceContext, recordTime);
  }
}
