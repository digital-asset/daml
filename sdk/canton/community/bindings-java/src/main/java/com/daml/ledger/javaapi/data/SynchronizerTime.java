// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.OffsetCheckpointOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Objects;

public final class SynchronizerTime {

  private final String synchronizerId;

  private final Instant recordTime;

  public SynchronizerTime(@NonNull String synchronizerId, @NonNull Instant recordTime) {
    this.synchronizerId = synchronizerId;
    this.recordTime = recordTime;
  }

  public static SynchronizerTime fromProto(
      OffsetCheckpointOuterClass.SynchronizerTime synchronizerTime) {

    return new SynchronizerTime(
        synchronizerTime.getSynchronizerId(),
        Instant.ofEpochSecond(
            synchronizerTime.getRecordTime().getSeconds(),
            synchronizerTime.getRecordTime().getNanos()));
  }

  public OffsetCheckpointOuterClass.SynchronizerTime toProto() {
    return OffsetCheckpointOuterClass.SynchronizerTime.newBuilder()
        .setSynchronizerId(this.synchronizerId)
        .setRecordTime(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(this.recordTime.getEpochSecond())
                .setNanos(this.recordTime.getNano())
                .build())
        .build();
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  public @NonNull Instant getRecordTime() {
    return recordTime;
  }

  @Override
  public String toString() {
    return "SynchronizerTime{"
        + "synchronizerId="
        + synchronizerId
        + ", recordTime="
        + recordTime
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SynchronizerTime that = (SynchronizerTime) o;
    return Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(recordTime, that.recordTime);
  }

  @Override
  public int hashCode() {

    return Objects.hash(synchronizerId, recordTime);
  }
}
