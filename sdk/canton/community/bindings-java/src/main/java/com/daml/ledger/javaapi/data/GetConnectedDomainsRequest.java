// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetConnectedDomainsRequest {
  private final @NonNull String party;

  public GetConnectedDomainsRequest(@NonNull String party) {
    this.party = party;
  }

  @NonNull
  String getParty() {
    return party;
  }

  public static GetConnectedDomainsRequest fromProto(
      StateServiceOuterClass.GetConnectedDomainsRequest request) {
    return new GetConnectedDomainsRequest(request.getParty());
  }

  public StateServiceOuterClass.GetConnectedDomainsRequest toProto() {
    return StateServiceOuterClass.GetConnectedDomainsRequest.newBuilder().setParty(party).build();
  }

  @Override
  public String toString() {
    return "GetConnectedDomainsRequest{" + "party='" + party + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetConnectedDomainsRequest that = (GetConnectedDomainsRequest) o;
    return Objects.equals(party, that.party);
  }

  @Override
  public int hashCode() {
    return Objects.hash(party);
  }
}
