// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class GetUpdateTreesResponse {

  @NonNull private final Optional<TransactionTree> transactionTree;

  @NonNull private final Optional<Reassignment> reassignment;

  private GetUpdateTreesResponse(
      @NonNull Optional<TransactionTree> transactionTree,
      @NonNull Optional<Reassignment> reassignment) {
    this.transactionTree = transactionTree;
    this.reassignment = reassignment;
  }

  public GetUpdateTreesResponse(@NonNull TransactionTree transactionTree) {
    this(Optional.of(transactionTree), Optional.empty());
  }

  public GetUpdateTreesResponse(@NonNull Reassignment reassignment) {
    this(Optional.empty(), Optional.of(reassignment));
  }

  public Optional<TransactionTree> getTransactionTree() {
    return transactionTree;
  }

  @NonNull
  public Optional<Reassignment> getReassignment() {
    return reassignment;
  }

  public static GetUpdateTreesResponse fromProto(
      UpdateServiceOuterClass.GetUpdateTreesResponse response) {
    return new GetUpdateTreesResponse(
        response.hasTransactionTree()
            ? Optional.of(TransactionTree.fromProto(response.getTransactionTree()))
            : Optional.empty(),
        response.hasReassignment()
            ? Optional.of(Reassignment.fromProto(response.getReassignment()))
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
    GetUpdateTreesResponse that = (GetUpdateTreesResponse) o;
    return Objects.equals(transactionTree, that.transactionTree)
        && Objects.equals(reassignment, that.reassignment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transactionTree, reassignment);
  }
}
