// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class IncompleteAssignedV2 implements ContractEntryV2 {

  private final @NonNull AssignedEventV2 assignedEvent;

  public IncompleteAssignedV2(@NonNull AssignedEventV2 assignedEvent) {
    this.assignedEvent = assignedEvent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IncompleteAssignedV2 that = (IncompleteAssignedV2) o;
    return Objects.equals(assignedEvent, that.assignedEvent);
  }

  @Override
  public int hashCode() {

    return Objects.hash(assignedEvent);
  }

  @Override
  public String toString() {
    return "IncompleteAssigned{" + "assignedEvent=" + assignedEvent + '}';
  }

  public StateServiceOuterClass.IncompleteAssigned toProto() {
    return StateServiceOuterClass.IncompleteAssigned.newBuilder()
        .setAssignedEvent(assignedEvent.toProto())
        .build();
  }

  public static IncompleteAssignedV2 fromProto(
      StateServiceOuterClass.IncompleteAssigned incompleteAssigned) {
    return new IncompleteAssignedV2(
        AssignedEventV2.fromProto(incompleteAssigned.getAssignedEvent()));
  }
}
