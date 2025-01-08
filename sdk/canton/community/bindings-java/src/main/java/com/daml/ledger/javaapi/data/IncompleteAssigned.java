// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class IncompleteAssigned implements ContractEntry {

  private final @NonNull AssignedEvent assignedEvent;

  public IncompleteAssigned(@NonNull AssignedEvent assignedEvent) {
    this.assignedEvent = assignedEvent;
  }

  @NonNull
  @Override
  public CreatedEvent getCreatedEvent() {
    return assignedEvent.getCreatedEvent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IncompleteAssigned that = (IncompleteAssigned) o;
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

  public static IncompleteAssigned fromProto(
      StateServiceOuterClass.IncompleteAssigned incompleteAssigned) {
    return new IncompleteAssigned(AssignedEvent.fromProto(incompleteAssigned.getAssignedEvent()));
  }
}
