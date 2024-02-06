// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class IncompleteUnassigned implements ContractEntry {

  private final @NonNull CreatedEvent createdEvent;

  private final @NonNull UnassignedEvent unassignedEvent;

  public IncompleteUnassigned(
      @NonNull CreatedEvent createdEvent, @NonNull UnassignedEvent unassignedEvent) {
    this.createdEvent = createdEvent;
    this.unassignedEvent = unassignedEvent;
  }

  @NonNull
  @Override
  public CreatedEvent getCreatedEvent() {
    return createdEvent;
  }

  @NonNull
  UnassignedEvent getUnassignedEvent() {
    return unassignedEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IncompleteUnassigned that = (IncompleteUnassigned) o;
    return Objects.equals(createdEvent, that.createdEvent)
        && Objects.equals(unassignedEvent, that.unassignedEvent);
  }

  @Override
  public int hashCode() {

    return Objects.hash(createdEvent, unassignedEvent);
  }

  @Override
  public String toString() {
    return "IncompleteUnassigned{"
        + "createdEvent="
        + createdEvent
        + ", unassignedEvent="
        + unassignedEvent
        + '}';
  }

  public StateServiceOuterClass.IncompleteUnassigned toProto() {
    return StateServiceOuterClass.IncompleteUnassigned.newBuilder()
        .setCreatedEvent(this.createdEvent.toProto())
        .setUnassignedEvent(this.unassignedEvent.toProto())
        .build();
  }

  public static IncompleteUnassigned fromProto(
      StateServiceOuterClass.IncompleteUnassigned incompleteUnassigned) {
    return new IncompleteUnassigned(
        CreatedEvent.fromProto(incompleteUnassigned.getCreatedEvent()),
        UnassignedEvent.fromProto(incompleteUnassigned.getUnassignedEvent()));
  }
}
