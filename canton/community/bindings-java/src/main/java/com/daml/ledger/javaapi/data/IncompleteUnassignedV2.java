// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class IncompleteUnassignedV2 implements ContractEntryV2 {

  private final @NonNull CreatedEvent createdEvent;

  private final @NonNull UnassignedEventV2 unassignedEvent;

  public IncompleteUnassignedV2(
      @NonNull CreatedEvent createdEvent, @NonNull UnassignedEventV2 unassignedEvent) {
    this.createdEvent = createdEvent;
    this.unassignedEvent = unassignedEvent;
  }

  @NonNull
  @Override
  public CreatedEvent getCreatedEvent() {
    return createdEvent;
  }

  @NonNull
  UnassignedEventV2 getUnassignedEvent() {
    return unassignedEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IncompleteUnassignedV2 that = (IncompleteUnassignedV2) o;
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

  public static IncompleteUnassignedV2 fromProto(
      StateServiceOuterClass.IncompleteUnassigned incompleteUnassigned) {
    return new IncompleteUnassignedV2(
        CreatedEvent.fromProto(incompleteUnassigned.getCreatedEvent()),
        UnassignedEventV2.fromProto(incompleteUnassigned.getUnassignedEvent()));
  }
}
