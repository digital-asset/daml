// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.OffsetCheckpointOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class OffsetCheckpoint {

  private final String offset;

  private final List<DomainTime> domainTimes;

  public OffsetCheckpoint(@NonNull String offset, @NonNull List<DomainTime> domainTimes) {
    this.offset = offset;
    this.domainTimes = domainTimes;
  }

  public static OffsetCheckpoint fromProto(
      OffsetCheckpointOuterClass.OffsetCheckpoint offsetCheckpoint) {

    return new OffsetCheckpoint(
        offsetCheckpoint.getOffset(),
        offsetCheckpoint.getDomainTimesList().stream()
            .map(DomainTime::fromProto)
            .collect(Collectors.toList()));
  }

  public OffsetCheckpointOuterClass.OffsetCheckpoint toProto() {
    return OffsetCheckpointOuterClass.OffsetCheckpoint.newBuilder()
        .setOffset(this.offset)
        .addAllDomainTimes(
            this.domainTimes.stream().map(DomainTime::toProto).collect(Collectors.toList()))
        .build();
  }

  @NonNull
  public String getOffset() {
    return offset;
  }

  @NonNull
  public List<@NonNull DomainTime> getDomainTimes() {
    return domainTimes;
  }

  @Override
  public String toString() {
    return "OffsetCheckpoint{" + "offset=" + offset + ", domainTimes=" + domainTimes + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OffsetCheckpoint that = (OffsetCheckpoint) o;
    return Objects.equals(offset, that.offset) && Objects.equals(domainTimes, that.domainTimes);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset, domainTimes);
  }
}
