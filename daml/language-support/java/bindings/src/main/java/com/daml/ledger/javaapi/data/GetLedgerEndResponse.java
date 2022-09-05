// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.TransactionServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public class GetLedgerEndResponse {

  private final LedgerOffset offset;

  public GetLedgerEndResponse(@NonNull LedgerOffset offset) {
    this.offset = offset;
  }

  public static GetLedgerEndResponse fromProto(
      TransactionServiceOuterClass.GetLedgerEndResponse response) {
    return new GetLedgerEndResponse(LedgerOffset.fromProto(response.getOffset()));
  }

  public TransactionServiceOuterClass.GetLedgerEndResponse toProto() {
    return TransactionServiceOuterClass.GetLedgerEndResponse.newBuilder()
        .setOffset(this.offset.toProto())
        .build();
  }

  @NonNull
  public LedgerOffset getOffset() {
    return offset;
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
