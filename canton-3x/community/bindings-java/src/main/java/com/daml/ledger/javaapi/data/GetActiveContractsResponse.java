// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ActiveContractsServiceOuterClass;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetActiveContractsResponse implements WorkflowEvent {

  private final String offset;

  private final java.util.List<CreatedEvent> activeContracts;

  private final String workflowId;

  public GetActiveContractsResponse(
      @NonNull String offset, @NonNull List<CreatedEvent> activeContracts, String workflowId) {
    this.offset = offset;
    this.activeContracts = activeContracts;
    this.workflowId = workflowId;
  }

  public static GetActiveContractsResponse fromProto(
      ActiveContractsServiceOuterClass.GetActiveContractsResponse response) {
    List<CreatedEvent> events =
        response.getActiveContractsList().stream()
            .map(CreatedEvent::fromProto)
            .collect(Collectors.toList());
    return new GetActiveContractsResponse(response.getOffset(), events, response.getWorkflowId());
  }

  public ActiveContractsServiceOuterClass.GetActiveContractsResponse toProto() {
    return ActiveContractsServiceOuterClass.GetActiveContractsResponse.newBuilder()
        .setOffset(this.offset)
        .addAllActiveContracts(
            this.activeContracts.stream().map(CreatedEvent::toProto).collect(Collectors.toList()))
        .setWorkflowId(this.workflowId)
        .build();
  }

  @NonNull
  public Optional<String> getOffset() {
    // Empty string indicates that the field is not present in the protobuf.
    return Optional.of(offset).filter(off -> !offset.equals(""));
  }

  @NonNull
  public List<@NonNull CreatedEvent> getCreatedEvents() {
    return activeContracts;
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public String toString() {
    return "GetActiveContractsResponse{"
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
    GetActiveContractsResponse that = (GetActiveContractsResponse) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(activeContracts, that.activeContracts)
        && Objects.equals(workflowId, that.workflowId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset, activeContracts, workflowId);
  }
}
