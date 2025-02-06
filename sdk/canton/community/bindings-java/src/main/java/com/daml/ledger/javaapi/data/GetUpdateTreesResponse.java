// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdateTreesResponse {

  @NonNull private final Optional<TransactionTree> transactionTree;

  @NonNull private final Optional<Reassignment> reassignment;

  @NonNull private final Optional<OffsetCheckpoint> offsetCheckpoint;

  @NonNull private final Optional<TopologyTransaction> topologyTransaction;

  private GetUpdateTreesResponse(
      @NonNull Optional<TransactionTree> transactionTree,
      @NonNull Optional<Reassignment> reassignment,
      @NonNull Optional<OffsetCheckpoint> offsetCheckpoint,
      @NonNull Optional<TopologyTransaction> topologyTransaction) {
    this.transactionTree = transactionTree;
    this.reassignment = reassignment;
    this.offsetCheckpoint = offsetCheckpoint;
    this.topologyTransaction = topologyTransaction;
  }

  public GetUpdateTreesResponse(@NonNull TransactionTree transactionTree) {
    this(Optional.of(transactionTree), Optional.empty(), Optional.empty(), Optional.empty());
  }

  public GetUpdateTreesResponse(@NonNull Reassignment reassignment) {
    this(Optional.empty(), Optional.of(reassignment), Optional.empty(), Optional.empty());
  }

  public GetUpdateTreesResponse(@NonNull OffsetCheckpoint offsetCheckpoint) {
    this(Optional.empty(), Optional.empty(), Optional.of(offsetCheckpoint), Optional.empty());
  }

  public GetUpdateTreesResponse(@NonNull TopologyTransaction topologyTransaction) {
    this(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(topologyTransaction));
  }

  @NonNull
  public Optional<TransactionTree> getTransactionTree() {
    return transactionTree;
  }

  @NonNull
  public Optional<Reassignment> getReassignment() {
    return reassignment;
  }

  @NonNull
  public Optional<OffsetCheckpoint> getOffsetCheckpoint() {
    return offsetCheckpoint;
  }

  @NonNull
  public Optional<TopologyTransaction> getTopologyTransaction() {
    return topologyTransaction;
  }

  public static GetUpdateTreesResponse fromProto(
      UpdateServiceOuterClass.GetUpdateTreesResponse response) {
    return new GetUpdateTreesResponse(
        response.hasTransactionTree()
            ? Optional.of(TransactionTree.fromProto(response.getTransactionTree()))
            : Optional.empty(),
        response.hasReassignment()
            ? Optional.of(Reassignment.fromProto(response.getReassignment()))
            : Optional.empty(),
        response.hasOffsetCheckpoint()
            ? Optional.of(OffsetCheckpoint.fromProto(response.getOffsetCheckpoint()))
            : Optional.empty(),
        response.hasTopologyTransaction()
            ? Optional.of(TopologyTransaction.fromProto(response.getTopologyTransaction()))
            : Optional.empty());
  }

  public UpdateServiceOuterClass.GetUpdateTreesResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdateTreesResponse.newBuilder();
    transactionTree.ifPresent(t -> builder.setTransactionTree(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    offsetCheckpoint.ifPresent(c -> builder.setOffsetCheckpoint(c.toProto()));
    topologyTransaction.ifPresent(t -> builder.setTopologyTransaction(t.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetUpdateTreesResponse{"
        + "transactionTree="
        + transactionTree
        + ", reassignment="
        + reassignment
        + ", offsetCheckpoint="
        + offsetCheckpoint
        + ", topologyTransaction="
        + topologyTransaction
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateTreesResponse that = (GetUpdateTreesResponse) o;
    return Objects.equals(transactionTree, that.transactionTree)
        && Objects.equals(reassignment, that.reassignment)
        && Objects.equals(offsetCheckpoint, that.offsetCheckpoint)
        && Objects.equals(topologyTransaction, that.topologyTransaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTree, reassignment, offsetCheckpoint, topologyTransaction);
  }
}
