// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ReassignmentOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class AssignedEvent implements ReassignmentEvent {

  private final @NonNull String source;

  private final @NonNull String target;

  private final @NonNull String reassignmentId;

  private final @NonNull String submitter;

  private final long reassignmentCounter;

  private final @NonNull CreatedEvent createdEvent;

  public AssignedEvent(
      @NonNull String source,
      @NonNull String target,
      @NonNull String reassignmentId,
      @NonNull String submitter,
      long reassignmentCounter,
      @NonNull CreatedEvent createdEvent) {
    this.source = source;
    this.target = target;
    this.reassignmentId = reassignmentId;
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
  public String getReassignmentId() {
    return reassignmentId;
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
    AssignedEvent that = (AssignedEvent) o;
    return Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && Objects.equals(reassignmentId, that.reassignmentId)
        && Objects.equals(submitter, that.submitter)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter)
        && Objects.equals(createdEvent, that.createdEvent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        source, target, reassignmentId, submitter, reassignmentCounter, createdEvent);
  }

  @Override
  public String toString() {
    return "AssignedEvent{"
        + "source="
        + source
        + ", target="
        + target
        + ", reassignmentId='"
        + reassignmentId
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
    return ReassignmentOuterClass.AssignedEvent.newBuilder()
        .setSource(this.source)
        .setTarget(this.target)
        .setReassignmentId(this.reassignmentId)
        .setSubmitter(this.submitter)
        .setReassignmentCounter(this.reassignmentCounter)
        .setCreatedEvent(this.getCreatedEvent().toProto())
        .build();
  }

  public static AssignedEvent fromProto(ReassignmentOuterClass.AssignedEvent assignedEvent) {
    return new AssignedEvent(
        assignedEvent.getSource(),
        assignedEvent.getTarget(),
        assignedEvent.getReassignmentId(),
        assignedEvent.getSubmitter(),
        assignedEvent.getReassignmentCounter(),
        CreatedEvent.fromProto(assignedEvent.getCreatedEvent()));
  }
}
