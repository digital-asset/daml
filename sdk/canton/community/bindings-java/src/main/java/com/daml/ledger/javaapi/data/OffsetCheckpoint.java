// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.OffsetCheckpointOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class OffsetCheckpoint {

  private final Long offset;

  private final List<SynchronizerTime> synchronizerTimes;

  public OffsetCheckpoint(@NonNull Long offset, @NonNull List<SynchronizerTime> synchronizerTimes) {
    this.offset = offset;
    this.synchronizerTimes = synchronizerTimes;
  }

  public static OffsetCheckpoint fromProto(
      OffsetCheckpointOuterClass.OffsetCheckpoint offsetCheckpoint) {

    return new OffsetCheckpoint(
        offsetCheckpoint.getOffset(),
        offsetCheckpoint.getSynchronizerTimesList().stream()
            .map(SynchronizerTime::fromProto)
            .collect(Collectors.toList()));
  }

  public OffsetCheckpointOuterClass.OffsetCheckpoint toProto() {
    return OffsetCheckpointOuterClass.OffsetCheckpoint.newBuilder()
        .setOffset(this.offset)
        .addAllSynchronizerTimes(
            this.synchronizerTimes.stream()
                .map(SynchronizerTime::toProto)
                .collect(Collectors.toList()))
        .build();
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public List<@NonNull SynchronizerTime> getSynchronizerTimes() {
    return synchronizerTimes;
  }

  @Override
  public String toString() {
    return "OffsetCheckpoint{"
        + "offset="
        + offset
        + ", synchronizerTimes="
        + synchronizerTimes
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OffsetCheckpoint that = (OffsetCheckpoint) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(synchronizerTimes, that.synchronizerTimes);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset, synchronizerTimes);
  }
}
