// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class GetUpdateTreesResponseV2 {

  @NonNull private final Optional<TransactionTreeV2> transactionTree;

  @NonNull private final Optional<ReassignmentV2> reassignment;

  private GetUpdateTreesResponseV2(
      @NonNull Optional<TransactionTreeV2> transactionTree,
      @NonNull Optional<ReassignmentV2> reassignment) {
    this.transactionTree = transactionTree;
    this.reassignment = reassignment;
  }

  public GetUpdateTreesResponseV2(@NonNull TransactionTreeV2 transactionTree) {
    this(Optional.of(transactionTree), Optional.empty());
  }

  public GetUpdateTreesResponseV2(@NonNull ReassignmentV2 reassignment) {
    this(Optional.empty(), Optional.of(reassignment));
  }

  public Optional<TransactionTreeV2> getTransactionTree() {
    return transactionTree;
  }

  @NonNull
  public Optional<ReassignmentV2> getReassignment() {
    return reassignment;
  }

  public static GetUpdateTreesResponseV2 fromProto(
      UpdateServiceOuterClass.GetUpdateTreesResponse response) {
    return new GetUpdateTreesResponseV2(
        response.hasTransactionTree()
            ? Optional.of(TransactionTreeV2.fromProto(response.getTransactionTree()))
            : Optional.empty(),
        response.hasReassignment()
            ? Optional.of(ReassignmentV2.fromProto(response.getReassignment()))
            : Optional.empty());
  }

  public UpdateServiceOuterClass.GetUpdateTreesResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdateTreesResponse.newBuilder();
    transactionTree.ifPresent(t -> builder.setTransactionTree(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetUpdateTreesResponse{"
        + "transactionTree="
        + transactionTree
        + ", reassignment="
        + reassignment
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateTreesResponseV2 that = (GetUpdateTreesResponseV2) o;
    return Objects.equals(transactionTree, that.transactionTree)
        && Objects.equals(reassignment, that.reassignment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTree, reassignment);
  }
}
