// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetConnectedDomainsRequestV2 {
  private final @NonNull String party;

  public GetConnectedDomainsRequestV2(@NonNull String party) {
    this.party = party;
  }

  @NonNull
  String getParty() {
    return party;
  }

  public static GetConnectedDomainsRequestV2 fromProto(
      StateServiceOuterClass.GetConnectedDomainsRequest request) {
    return new GetConnectedDomainsRequestV2(request.getParty());
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
    GetConnectedDomainsRequestV2 that = (GetConnectedDomainsRequestV2) o;
    return Objects.equals(party, that.party);
  }

  @Override
  public int hashCode() {
    return Objects.hash(party);
  }
}
