// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ActiveContracts<Ct> {

  public final List<Ct> activeContracts;

  public final String workflowId;

  public ActiveContracts(
      @NonNull List<Ct> activeContracts,
      @NonNull String workflowId) {
    this.activeContracts = activeContracts;
    this.workflowId = workflowId;
  }

  @Override
  public String toString() {
    return "ActiveContracts{"
        + "activeContracts="
        + activeContracts
        + ", workflowId="
        + workflowId
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ActiveContracts<?> that = (ActiveContracts<?>) o;
    return Objects.equals(activeContracts, that.activeContracts)
        && Objects.equals(workflowId, that.workflowId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeContracts, workflowId);
  }
}
