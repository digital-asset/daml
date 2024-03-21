// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.CommandCompletionServiceOuterClass;
import java.time.Instant;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public class Checkpoint {

  private final Instant recordTime;

  private final LedgerOffset offset;

  public Checkpoint(@NonNull Instant recordTime, @NonNull LedgerOffset offset) {
    this.recordTime = recordTime;
    this.offset = offset;
  }

  public static Checkpoint fromProto(CommandCompletionServiceOuterClass.Checkpoint checkpoint) {
    LedgerOffset offset = LedgerOffset.fromProto(checkpoint.getOffset());
    return new Checkpoint(
        Instant.ofEpochSecond(
            checkpoint.getRecordTime().getSeconds(), checkpoint.getRecordTime().getNanos()),
        offset);
  }

  public CommandCompletionServiceOuterClass.Checkpoint toProto() {
    return CommandCompletionServiceOuterClass.Checkpoint.newBuilder()
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
  public LedgerOffset getOffset() {
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
