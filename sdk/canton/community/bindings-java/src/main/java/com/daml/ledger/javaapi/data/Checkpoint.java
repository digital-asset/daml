// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CheckpointOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Objects;

public final class Checkpoint {

  private final Instant recordTime;

  private final ParticipantOffset offset;

  public Checkpoint(@NonNull Instant recordTime, @NonNull ParticipantOffset offset) {
    this.recordTime = recordTime;
    this.offset = offset;
  }

  public static Checkpoint fromProto(CheckpointOuterClass.Checkpoint checkpoint) {
    ParticipantOffset offset = ParticipantOffset.fromProto(checkpoint.getOffset());
    return new Checkpoint(
        Instant.ofEpochSecond(
            checkpoint.getRecordTime().getSeconds(), checkpoint.getRecordTime().getNanos()),
        offset);
  }

  public CheckpointOuterClass.Checkpoint toProto() {
    return CheckpointOuterClass.Checkpoint.newBuilder()
        .setRecordTime(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(this.recordTime.getEpochSecond())
                .setNanos(this.recordTime.getNano())
                .build())
        .setOffset(this.offset.toProto())
        .build();
  }

  public @NonNull Instant getRecordTime() {
    return recordTime;
  }

  @NonNull
  public ParticipantOffset getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "Checkpoint{" + "recordTime=" + recordTime + ", offset=" + offset + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Checkpoint that = (Checkpoint) o;
    return Objects.equals(recordTime, that.recordTime) && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {

    return Objects.hash(recordTime, offset);
  }
}
