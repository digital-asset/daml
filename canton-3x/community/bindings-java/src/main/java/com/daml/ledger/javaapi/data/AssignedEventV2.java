// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class AssignedEventV2 {

  private final @NonNull String source;

  private final @NonNull String target;

  private final @NonNull String unassignId;

  private final @NonNull String submitter;

  private final long reassignmentCounter;

  private final @NonNull CreatedEvent createdEvent;

  public AssignedEventV2(
      @NonNull String source,
      @NonNull String target,
      @NonNull String unassignId,
      @NonNull String submitter,
      long reassignmentCounter,
      @NonNull CreatedEvent createdEvent) {
    this.source = source;
    this.target = target;
    this.unassignId = unassignId;
    this.submitter = submitter;
    this.reassignmentCounter = reassignmentCounter;
    this.createdEvent = createdEvent;
  }

  @NonNull
  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  @NonNull
  public String getUnassignId() {
    return unassignId;
  }

  @NonNull
  public String getSubmitter() {
    return submitter;
  }

  public long getReassignmentCounter() {
    return reassignmentCounter;
  }

  @NonNull
  public CreatedEvent getCreatedEvent() {
    return createdEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AssignedEventV2 that = (AssignedEventV2) o;
    return Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(unassignId, that.unassignId)
        && Objects.equals(submitter, that.submitter)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter)
        && Objects.equals(createdEvent, that.createdEvent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target, unassignId, submitter, reassignmentCounter, createdEvent);
  }

  @Override
  public String toString() {
    return "AssignedEvent{"
        + "source="
        + source
        + ", target="
        + target
        + ", unassignId='"
        + unassignId
        + '\''
        + ", submitter="
        + submitter
        + ", reassignmentCounter="
        + reassignmentCounter
        + ", createdEvent="
        + createdEvent
        + '}';
  }

  public ReassignmentOuterClass.AssignedEvent toProto() {
    ReassignmentOuterClass.AssignedEvent.Builder builder =
        ReassignmentOuterClass.AssignedEvent.newBuilder()
            .setSource(this.source)
            .setTarget(this.target)
            .setUnassignId(this.unassignId)
            .setSubmitter(this.submitter)
            .setReassignmentCounter(this.reassignmentCounter)
            .setCreatedEvent(this.getCreatedEvent().toProto());
    return builder.build();
  }

  public static AssignedEventV2 fromProto(ReassignmentOuterClass.AssignedEvent assignedEvent) {
    return new AssignedEventV2(
        assignedEvent.getSource(),
        assignedEvent.getTarget(),
        assignedEvent.getUnassignId(),
        assignedEvent.getSubmitter(),
        assignedEvent.getReassignmentCounter(),
        CreatedEvent.fromProto(assignedEvent.getCreatedEvent()));
  }
}
