// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// TODO (i15873) Eliminate V2 suffix
public final class GetConnectedDomainsResponseV2 {
  private final @NonNull List<@NonNull ConnectedDomainV2> connectedDomains;

  public GetConnectedDomainsResponseV2(@NonNull List<@NonNull ConnectedDomainV2> connectedDomains) {
    this.connectedDomains = List.copyOf(connectedDomains);
  }

  @NonNull
  List<@NonNull ConnectedDomainV2> getConnectedDomains() {
    return connectedDomains;
  }

  public static GetConnectedDomainsResponseV2 fromProto(
      StateServiceOuterClass.GetConnectedDomainsResponse response) {
    return new GetConnectedDomainsResponseV2(
        response.getConnectedDomainsList().stream()
            .map(ConnectedDomainV2::fromProto)
            .collect(Collectors.toList()));
  }

  public StateServiceOuterClass.GetConnectedDomainsResponse toProto() {
    return StateServiceOuterClass.GetConnectedDomainsResponse.newBuilder()
        .addAllConnectedDomains(
            this.connectedDomains.stream()
                .map(ConnectedDomainV2::toProto)
                .collect(Collectors.toList()))
        .build();
  }

  @Override
  public String toString() {
    return "GetConnectedDomainsResponse{" + "connectedDomains=" + connectedDomains + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetConnectedDomainsResponseV2 that = (GetConnectedDomainsResponseV2) o;
    return Objects.equals(connectedDomains, that.connectedDomains);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectedDomains);
  }
}
