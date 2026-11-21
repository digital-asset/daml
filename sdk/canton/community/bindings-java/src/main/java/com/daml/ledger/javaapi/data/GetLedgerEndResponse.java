// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetLedgerEndResponse {

  @NonNull private final Long offset;

  public GetLedgerEndResponse(@NonNull Long offset) {
    this.offset = offset;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  public static GetLedgerEndResponse fromProto(
      StateServiceOuterClass.GetLedgerEndResponse response) {
    return new GetLedgerEndResponse(response.getOffset());
  }

  public StateServiceOuterClass.GetLedgerEndResponse toProto() {
    return StateServiceOuterClass.GetLedgerEndResponse.newBuilder().setOffset(this.offset).build();
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
