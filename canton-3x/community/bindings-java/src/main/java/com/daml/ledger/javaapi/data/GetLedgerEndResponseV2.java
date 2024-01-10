// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetLedgerEndResponseV2 {

  @NonNull private final ParticipantOffsetV2 offset;

  public GetLedgerEndResponseV2(@NonNull ParticipantOffsetV2 offset) {
    this.offset = offset;
  }

  @NonNull
  public ParticipantOffsetV2 getOffset() {
    return offset;
  }

  public static GetLedgerEndResponseV2 fromProto(
      StateServiceOuterClass.GetLedgerEndResponse response) {
    return new GetLedgerEndResponseV2(ParticipantOffsetV2.fromProto(response.getOffset()));
  }

  public StateServiceOuterClass.GetLedgerEndResponse toProto() {
    return StateServiceOuterClass.GetLedgerEndResponse.newBuilder()
        .setOffset(this.offset.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "GetLedgerEndResponse{" + "offset=" + offset + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetLedgerEndResponseV2 that = (GetLedgerEndResponseV2) o;
    return Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset);
  }
}
