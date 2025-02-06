// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetActiveContractsResponse implements WorkflowEvent {

  private final Optional<ContractEntry> contractEntry;

  private final String workflowId;

  public GetActiveContractsResponse(
      @NonNull Optional<ContractEntry> contractEntry, String workflowId) {
    this.contractEntry = contractEntry;
    this.workflowId = workflowId;
  }

  public static GetActiveContractsResponse fromProto(
      StateServiceOuterClass.GetActiveContractsResponse response) {
    switch (response.getContractEntryCase()) {
      case ACTIVE_CONTRACT:
        return new GetActiveContractsResponse(
            Optional.of(ActiveContract.fromProto(response.getActiveContract())),
            response.getWorkflowId());
      case INCOMPLETE_UNASSIGNED:
        return new GetActiveContractsResponse(
            Optional.of(IncompleteUnassigned.fromProto(response.getIncompleteUnassigned())),
            response.getWorkflowId());
      case INCOMPLETE_ASSIGNED:
        return new GetActiveContractsResponse(
            Optional.of(IncompleteAssigned.fromProto(response.getIncompleteAssigned())),
            response.getWorkflowId());
      case CONTRACTENTRY_NOT_SET:
        return new GetActiveContractsResponse(Optional.empty(), response.getWorkflowId());
      default:
        throw new ProtoContractEntryUnknown(response);
    }
  }

  public StateServiceOuterClass.GetActiveContractsResponse toProto() {
    var builder =
        StateServiceOuterClass.GetActiveContractsResponse.newBuilder()
            .setWorkflowId(this.workflowId);
    if (contractEntry.isPresent()) {
      ContractEntry ce = contractEntry.get();
      if (ce instanceof ActiveContract) builder.setActiveContract(((ActiveContract) ce).toProto());
      else if (ce instanceof IncompleteUnassigned)
        builder.setIncompleteUnassigned(((IncompleteUnassigned) ce).toProto());
      else if (ce instanceof IncompleteAssigned)
        builder.setIncompleteAssigned(((IncompleteAssigned) ce).toProto());
    }
    return builder.build();
  }

  public Optional<ContractEntry> getContractEntry() {
    return contractEntry;
  }

  @NonNull
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public String toString() {
    return "GetActiveContractsResponse{"
        + "contractEntry="
        + contractEntry
        + ", workflowId="
        + workflowId
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetActiveContractsResponse that = (GetActiveContractsResponse) o;
    return Objects.equals(contractEntry, that.contractEntry)
        && Objects.equals(workflowId, that.workflowId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(contractEntry, workflowId);
  }
}

class ProtoContractEntryUnknown extends RuntimeException {
  public ProtoContractEntryUnknown(StateServiceOuterClass.GetActiveContractsResponse response) {
    super("ContractEntry unknown " + response.toString());
  }
}
