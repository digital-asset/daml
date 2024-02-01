// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class GetActiveContractsResponseV2 implements WorkflowEvent {

  private final String offset;

  private final Optional<ContractEntryV2> contractEntry;

  private final String workflowId;

  public GetActiveContractsResponseV2(
      @NonNull String offset, @NonNull Optional<ContractEntryV2> contractEntry, String workflowId) {
    this.offset = offset;
    this.contractEntry = contractEntry;
    this.workflowId = workflowId;
  }

  public static GetActiveContractsResponseV2 fromProto(
      StateServiceOuterClass.GetActiveContractsResponse response) {
    switch (response.getContractEntryCase()) {
      case ACTIVE_CONTRACT:
        return new GetActiveContractsResponseV2(
            response.getOffset(),
            Optional.of(ActiveContractV2.fromProto(response.getActiveContract())),
            response.getWorkflowId());
      case INCOMPLETE_UNASSIGNED:
        return new GetActiveContractsResponseV2(
            response.getOffset(),
            Optional.of(IncompleteUnassignedV2.fromProto(response.getIncompleteUnassigned())),
            response.getWorkflowId());
      case INCOMPLETE_ASSIGNED:
        return new GetActiveContractsResponseV2(
            response.getOffset(),
            Optional.of(IncompleteAssignedV2.fromProto(response.getIncompleteAssigned())),
            response.getWorkflowId());
      case CONTRACTENTRY_NOT_SET:
        return new GetActiveContractsResponseV2(
            response.getOffset(), Optional.empty(), response.getWorkflowId());
      default:
        throw new ProtoContractEntryUnknown(response);
    }
  }

  public StateServiceOuterClass.GetActiveContractsResponse toProto() {
    var builder =
        StateServiceOuterClass.GetActiveContractsResponse.newBuilder()
            .setOffset(this.offset)
            .setWorkflowId(this.workflowId);
    if (contractEntry.isPresent()) {
      ContractEntryV2 ce = contractEntry.get();
      if (ce instanceof ActiveContractV2)
        builder.setActiveContract(((ActiveContractV2) ce).toProto());
      else if (ce instanceof IncompleteUnassignedV2)
        builder.setIncompleteUnassigned(((IncompleteUnassignedV2) ce).toProto());
      else if (ce instanceof IncompleteAssignedV2)
        builder.setIncompleteAssigned(((IncompleteAssignedV2) ce).toProto());
    }
    return builder.build();
  }

  @NonNull
  public Optional<String> getOffset() {
    // Empty string indicates that the field is not present in the protobuf.
    return Optional.of(offset).filter(off -> !offset.equals(""));
  }

  public Optional<ContractEntryV2> getContractEntry() {
    return contractEntry;
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
        + ", contractEntry="
        + contractEntry
        + ", workflowId="
        + workflowId
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetActiveContractsResponseV2 that = (GetActiveContractsResponseV2) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(contractEntry, that.contractEntry)
        && Objects.equals(workflowId, that.workflowId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, contractEntry, workflowId);
  }
}

class ProtoContractEntryUnknown extends RuntimeException {
  public ProtoContractEntryUnknown(StateServiceOuterClass.GetActiveContractsResponse response) {
    super("ContractEntry unknown " + response.toString());
  }
}
