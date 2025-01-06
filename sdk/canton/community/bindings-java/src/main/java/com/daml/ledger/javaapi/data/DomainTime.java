// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.OffsetCheckpointOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Objects;

public final class DomainTime {

  private final String synchronizerId;

  private final Instant recordTime;

  public DomainTime(@NonNull String synchronizerId, @NonNull Instant recordTime) {
    this.synchronizerId = synchronizerId;
    this.recordTime = recordTime;
  }

  public static DomainTime fromProto(OffsetCheckpointOuterClass.DomainTime domainTime) {

    return new DomainTime(
        domainTime.getSynchronizerId(),
        Instant.ofEpochSecond(
            domainTime.getRecordTime().getSeconds(), domainTime.getRecordTime().getNanos()));
  }

  public OffsetCheckpointOuterClass.DomainTime toProto() {
    return OffsetCheckpointOuterClass.DomainTime.newBuilder()
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
    return "DomainTime{" + "synchronizerId=" + synchronizerId + ", recordTime=" + recordTime + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DomainTime that = (DomainTime) o;
    return Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(recordTime, that.recordTime);
  }

  @Override
  public int hashCode() {

    return Objects.hash(synchronizerId, recordTime);
  }
}
