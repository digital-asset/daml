// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class ActiveContract implements ContractEntry {

  @NonNull private final CreatedEvent createdEvent;

  @NonNull private final String synchronizerId;

  private final long reassignmentCounter;

  public ActiveContract(
      @NonNull CreatedEvent createdEvent,
      @NonNull String synchronizerId,
      long reassignmentCounter) {
    this.createdEvent = createdEvent;
    this.synchronizerId = synchronizerId;
    this.reassignmentCounter = reassignmentCounter;
  }

  @NonNull
  @Override
  public CreatedEvent getCreatedEvent() {
    return createdEvent;
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
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
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(reassignmentCounter, that.reassignmentCounter);
  }

  @Override
  public int hashCode() {

    return Objects.hash(createdEvent, synchronizerId, reassignmentCounter);
  }

  @Override
  public String toString() {
    return "ActiveContract{"
        + "createdEvent="
        + createdEvent
        + ", synchronizerId='"
        + synchronizerId
        + '\''
        + ", reassignmentCounter="
        + reassignmentCounter
        + '}';
  }

  public StateServiceOuterClass.ActiveContract toProto() {
    return StateServiceOuterClass.ActiveContract.newBuilder()
        .setCreatedEvent(getCreatedEvent().toProto())
        .setSynchronizerId(getSynchronizerId())
        .setReassignmentCounter(getReassignmentCounter())
        .build();
  }

  public static ActiveContract fromProto(StateServiceOuterClass.ActiveContract activeContract) {
    return new ActiveContract(
        CreatedEvent.fromProto(activeContract.getCreatedEvent()),
        activeContract.getSynchronizerId(),
        activeContract.getReassignmentCounter());
  }
}
