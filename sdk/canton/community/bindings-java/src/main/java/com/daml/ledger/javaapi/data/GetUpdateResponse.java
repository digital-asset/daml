// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdateResponse {

  @NonNull private final Optional<Transaction> transaction;

  @NonNull private final Optional<Reassignment> reassignment;

  @NonNull private final Optional<TopologyTransaction> topologyTransaction;

  private GetUpdateResponse(
      @NonNull Optional<Transaction> transaction,
      @NonNull Optional<Reassignment> reassignment,
      @NonNull Optional<TopologyTransaction> topologyTransaction) {
    this.transaction = transaction;
    this.reassignment = reassignment;
    this.topologyTransaction = topologyTransaction;
  }

  public GetUpdateResponse(@NonNull Transaction transaction) {
    this(Optional.of(transaction), Optional.empty(), Optional.empty());
  }

  public GetUpdateResponse(@NonNull Reassignment reassignment) {
    this(Optional.empty(), Optional.of(reassignment), Optional.empty());
  }

  public GetUpdateResponse(@NonNull TopologyTransaction topologyTransaction) {
    this(Optional.empty(), Optional.empty(), Optional.of(topologyTransaction));
  }

  @NonNull
  public Optional<Transaction> getTransaction() {
    return transaction;
  }

  @NonNull
  public Optional<Reassignment> getReassignment() {
    return reassignment;
  }

  @NonNull
  public Optional<TopologyTransaction> getTopologyTransaction() {
    return topologyTransaction;
  }

  public static GetUpdateResponse fromProto(UpdateServiceOuterClass.GetUpdateResponse response) {
    return new GetUpdateResponse(
        response.hasTransaction()
            ? Optional.of(Transaction.fromProto(response.getTransaction()))
            : Optional.empty(),
        response.hasReassignment()
            ? Optional.of(Reassignment.fromProto(response.getReassignment()))
            : Optional.empty(),
        response.hasTopologyTransaction()
            ? Optional.of(TopologyTransaction.fromProto(response.getTopologyTransaction()))
            : Optional.empty());
  }

  public UpdateServiceOuterClass.GetUpdateResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdateResponse.newBuilder();
    transaction.ifPresent(t -> builder.setTransaction(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    topologyTransaction.ifPresent(t -> builder.setTopologyTransaction(t.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetUpdatesResponse{"
        + "transaction="
        + transaction
        + ", reassignment="
        + reassignment
        + ", topologyTransaction="
        + topologyTransaction
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdateResponse that = (GetUpdateResponse) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(reassignment, that.reassignment)
        && Objects.equals(topologyTransaction, that.topologyTransaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, reassignment, topologyTransaction);
  }
}
