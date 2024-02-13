// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ActiveContract implements ContractEntry {

  @NonNull private final CreatedEvent createdEvent;

  @NonNull private final String domainId;

  private final long reassignmentCounter;

  public ActiveContract(
      @NonNull CreatedEvent createdEvent, @NonNull String domainId, long reassignmentCounter) {
    this.createdEvent = createdEvent;
    this.domainId = domainId;
    this.reassignmentCounter = reassignmentCounter;
  }

  @NonNull
  @Override
  public CreatedEvent getCreatedEvent() {
    return createdEvent;
  }

  @NonNull
  public String getDomainId() {
    return domainId;
  }

  public long getReassignmentCounter() {
    return reassignmentCounter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ActiveContract that = (ActiveContract) o;
    return Objects.equals(createdEvent, that.createdEvent)
        && Objects.equals(domainId, that.domainId)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter);
  }

  @Override
  public int hashCode() {

    return Objects.hash(createdEvent, domainId, reassignmentCounter);
  }

  @Override
  public String toString() {
    return "ActiveContract{"
        + "createdEvent="
        + createdEvent
        + ", domainId='"
        + domainId
        + '\''
        + ", reassignmentCounter="
        + reassignmentCounter
        + '}';
  }

  public StateServiceOuterClass.ActiveContract toProto() {
    return StateServiceOuterClass.ActiveContract.newBuilder()
        .setCreatedEvent(getCreatedEvent().toProto())
        .setDomainId(getDomainId())
        .setReassignmentCounter(getReassignmentCounter())
        .build();
  }

  public static ActiveContract fromProto(StateServiceOuterClass.ActiveContract activeContract) {
    return new ActiveContract(
        CreatedEvent.fromProto(activeContract.getCreatedEvent()),
        activeContract.getDomainId(),
        activeContract.getReassignmentCounter());
  }
}
