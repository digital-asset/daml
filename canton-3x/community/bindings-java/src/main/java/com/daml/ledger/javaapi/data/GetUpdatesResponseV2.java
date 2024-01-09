// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class GetUpdatesResponseV2 {

  @NonNull private final Optional<TransactionV2> transaction;

  @NonNull private final Optional<ReassignmentV2> reassignment;

  private GetUpdatesResponseV2(
      @NonNull Optional<TransactionV2> transaction,
      @NonNull Optional<ReassignmentV2> reassignment) {
    this.transaction = transaction;
    this.reassignment = reassignment;
  }

  public GetUpdatesResponseV2(@NonNull TransactionV2 transaction) {
    this(Optional.of(transaction), Optional.empty());
  }

  public GetUpdatesResponseV2(@NonNull ReassignmentV2 reassignment) {
    this(Optional.empty(), Optional.of(reassignment));
  }

  public Optional<TransactionV2> getTransaction() {
    return transaction;
  }

  @NonNull
  public Optional<ReassignmentV2> getReassignment() {
    return reassignment;
  }

  public static GetUpdatesResponseV2 fromProto(
      UpdateServiceOuterClass.GetUpdatesResponse response) {
    return new GetUpdatesResponseV2(
        response.hasTransaction()
            ? Optional.of(TransactionV2.fromProto(response.getTransaction()))
            : Optional.empty(),
        response.hasReassignment()
            ? Optional.of(ReassignmentV2.fromProto(response.getReassignment()))
            : Optional.empty());
  }

  public UpdateServiceOuterClass.GetUpdatesResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdatesResponse.newBuilder();
    transaction.ifPresent(t -> builder.setTransaction(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetUpdatesResponse{"
        + "transaction="
        + transaction
        + ", reassignment="
        + reassignment
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesResponseV2 that = (GetUpdatesResponseV2) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(reassignment, that.reassignment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, reassignment);
  }
}
