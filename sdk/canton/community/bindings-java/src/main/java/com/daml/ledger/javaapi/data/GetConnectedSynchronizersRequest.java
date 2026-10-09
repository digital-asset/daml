// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetConnectedSynchronizersRequest {
  private final @NonNull String party;

  public GetConnectedSynchronizersRequest(@NonNull String party) {
    this.party = party;
  }

  @NonNull
  String getParty() {
    return party;
  }

  public static GetConnectedSynchronizersRequest fromProto(
      StateServiceOuterClass.GetConnectedSynchronizersRequest request) {
    return new GetConnectedSynchronizersRequest(request.getParty());
  }

  public StateServiceOuterClass.GetConnectedSynchronizersRequest toProto() {
    return StateServiceOuterClass.GetConnectedSynchronizersRequest.newBuilder()
        .setParty(party)
        .build();
  }

  @Override
  public String toString() {
    return "GetConnectedSynchronizersRequest{" + "party='" + party + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetConnectedSynchronizersRequest that = (GetConnectedSynchronizersRequest) o;
    return Objects.equals(party, that.party);
  }

  @Override
  public int hashCode() {
    return Objects.hash(party);
  }
}
