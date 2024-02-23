// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetLedgerEndResponse {

  @NonNull private final ParticipantOffset offset;

  public GetLedgerEndResponse(@NonNull ParticipantOffset offset) {
    this.offset = offset;
  }

  @NonNull
  public ParticipantOffset getOffset() {
    return offset;
  }

  public static GetLedgerEndResponse fromProto(
      StateServiceOuterClass.GetLedgerEndResponse response) {
    return new GetLedgerEndResponse(ParticipantOffset.fromProto(response.getOffset()));
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
    GetLedgerEndResponse that = (GetLedgerEndResponse) o;
    return Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {

    return Objects.hash(offset);
  }
}
