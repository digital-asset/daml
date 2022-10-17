// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ActiveContracts<Ct> implements WorkflowEvent {

  private final String offset;

  private final List<Ct> activeContracts;

  private final String workflowId;

  public ActiveContracts(
      @NonNull String offset, @NonNull List<Ct> activeContracts, String workflowId) {
    this.offset = offset;
    this.activeContracts = activeContracts;
    this.workflowId = workflowId;
  }

  @NonNull
  public Optional<String> getOffset() {
    // Empty string indicates that the field is not present in the protobuf.
    return Optional.of(offset).filter(off -> !offset.equals(""));
  }

  @NonNull
  public List<@NonNull Ct> getContracts() {
    return activeContracts;
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public String toString() {
    return "ActiveContracts{"
        + "offset='"
        + offset
        + '\''
        + ", activeContracts="
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
    return Objects.equals(offset, that.offset)
        && Objects.equals(activeContracts, that.activeContracts)
        && Objects.equals(workflowId, that.workflowId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, activeContracts, workflowId);
  }
}
