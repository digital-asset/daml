// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetUpdatesResponse {

  @NonNull private final Optional<Transaction> transaction;

  @NonNull private final Optional<Reassignment> reassignment;

  @NonNull private final Optional<OffsetCheckpoint> offsetCheckpoint;

  private GetUpdatesResponse(
      @NonNull Optional<Transaction> transaction,
      @NonNull Optional<Reassignment> reassignment,
      @NonNull Optional<OffsetCheckpoint> offsetCheckpoint) {
    this.transaction = transaction;
    this.reassignment = reassignment;
    this.offsetCheckpoint = offsetCheckpoint;
  }

  public GetUpdatesResponse(@NonNull Transaction transaction) {
    this(Optional.of(transaction), Optional.empty(), Optional.empty());
  }

  public GetUpdatesResponse(@NonNull Reassignment reassignment) {
    this(Optional.empty(), Optional.of(reassignment), Optional.empty());
  }

  public GetUpdatesResponse(@NonNull OffsetCheckpoint offsetCheckpoint) {
    this(Optional.empty(), Optional.empty(), Optional.of(offsetCheckpoint));
  }

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
            : Optional.empty());
  }

  public UpdateServiceOuterClass.GetUpdatesResponse toProto() {
    var builder = UpdateServiceOuterClass.GetUpdatesResponse.newBuilder();
    transaction.ifPresent(t -> builder.setTransaction(t.toProto()));
    reassignment.ifPresent(r -> builder.setReassignment(r.toProto()));
    offsetCheckpoint.ifPresent(c -> builder.setOffsetCheckpoint(c.toProto()));
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
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesResponse that = (GetUpdatesResponse) o;
    return Objects.equals(transaction, that.transaction)
        && Objects.equals(reassignment, that.reassignment)
        && Objects.equals(offsetCheckpoint, that.offsetCheckpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(transaction, reassignment, offsetCheckpoint);
  }
}
