// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdatesResponse {

  @NonNull private final Optional<Transaction> transaction;

  @NonNull private final Optional<Reassignment> reassignment;

  @NonNull private final Optional<OffsetCheckpoint> offsetCheckpoint;

  @NonNull private final Optional<TopologyTransaction> topologyTransaction;

  private GetUpdatesResponse(
      @NonNull Optional<Transaction> transaction,
      @NonNull Optional<Reassignment> reassignment,
      @NonNull Optional<OffsetCheckpoint> offsetCheckpoint,
      @NonNull Optional<TopologyTransaction> topologyTransaction) {
    this.transaction = transaction;
    this.reassignment = reassignment;
    this.offsetCheckpoint = offsetCheckpoint;
    this.topologyTransaction = topologyTransaction;
  }

  public GetUpdatesResponse(@NonNull Transaction transaction) {
    this(Optional.of(transaction), Optional.empty(), Optional.empty(), Optional.empty());
  }

  public GetUpdatesResponse(@NonNull Reassignment reassignment) {
    this(Optional.empty(), Optional.of(reassignment), Optional.empty(), Optional.empty());
  }

  public GetUpdatesResponse(@NonNull OffsetCheckpoint offsetCheckpoint) {
    this(Optional.empty(), Optional.empty(), Optional.of(offsetCheckpoint), Optional.empty());
  }

  public GetUpdatesResponse(@NonNull TopologyTransaction topologyTransaction) {
    this(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(topologyTransaction));
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
  public Optional<OffsetCheckpoint> getOffsetCheckpoint() {
    return offsetCheckpoint;
  }

  @NonNull
  public Optional<TopologyTransaction> getTopologyTransaction() {
    return topologyTransaction;
  }

  public static GetUpdatesResponse fromProto(UpdateServiceOuterClass.GetUpdatesResponse response) {
    return new GetUpdatesResponse(
        response.hasTransaction()
            ? Optional.of(Transaction.fromProto(response.getTransaction()))
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

  public UpdateServiceOuterClass.GetUpdatesResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdatesResponse.newBuilder();
    transaction.ifPresent(t -> builder.setTransaction(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    offsetCheckpoint.ifPresent(c -> builder.setOffsetCheckpoint(c.toProto()));
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
    GetUpdatesResponse that = (GetUpdatesResponse) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(reassignment, that.reassignment)
        && Objects.equals(offsetCheckpoint, that.offsetCheckpoint)
        && Objects.equals(topologyTransaction, that.topologyTransaction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, reassignment, offsetCheckpoint, topologyTransaction);
  }
}
