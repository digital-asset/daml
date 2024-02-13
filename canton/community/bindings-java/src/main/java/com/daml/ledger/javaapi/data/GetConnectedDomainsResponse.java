// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GetConnectedDomainsResponse {
  private final @NonNull List<@NonNull ConnectedDomain> connectedDomains;

  public GetConnectedDomainsResponse(@NonNull List<@NonNull ConnectedDomain> connectedDomains) {
    this.connectedDomains = List.copyOf(connectedDomains);
  }

  @NonNull
  List<@NonNull ConnectedDomain> getConnectedDomains() {
    return connectedDomains;
  }

  public static GetConnectedDomainsResponse fromProto(
      StateServiceOuterClass.GetConnectedDomainsResponse response) {
    return new GetConnectedDomainsResponse(
        response.getConnectedDomainsList().stream()
            .map(ConnectedDomain::fromProto)
            .collect(Collectors.toList()));
  }

  public StateServiceOuterClass.GetConnectedDomainsResponse toProto() {
    return StateServiceOuterClass.GetConnectedDomainsResponse.newBuilder()
        .addAllConnectedDomains(
            this.connectedDomains.stream()
                .map(ConnectedDomain::toProto)
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
    GetConnectedDomainsResponse that = (GetConnectedDomainsResponse) o;
    return Objects.equals(connectedDomains, that.connectedDomains);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectedDomains);
  }
}
