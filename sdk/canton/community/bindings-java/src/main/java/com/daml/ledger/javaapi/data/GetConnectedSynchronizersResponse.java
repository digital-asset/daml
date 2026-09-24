// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GetConnectedSynchronizersResponse {
  private final @NonNull List<@NonNull ConnectedSynchronizer> connectedSynchronizers;

  public GetConnectedSynchronizersResponse(
      @NonNull List<@NonNull ConnectedSynchronizer> connectedSynchronizers) {
    this.connectedSynchronizers = List.copyOf(connectedSynchronizers);
  }

  @NonNull
  List<@NonNull ConnectedSynchronizer> getConnectedSynchronizers() {
    return connectedSynchronizers;
  }

  public static GetConnectedSynchronizersResponse fromProto(
      StateServiceOuterClass.GetConnectedSynchronizersResponse response) {
    return new GetConnectedSynchronizersResponse(
        response.getConnectedSynchronizersList().stream()
            .map(ConnectedSynchronizer::fromProto)
            .collect(Collectors.toList()));
  }

  public StateServiceOuterClass.GetConnectedSynchronizersResponse toProto() {
    return StateServiceOuterClass.GetConnectedSynchronizersResponse.newBuilder()
        .addAllConnectedSynchronizers(
            this.connectedSynchronizers.stream()
                .map(ConnectedSynchronizer::toProto)
                .collect(Collectors.toList()))
        .build();
  }

  @Override
  public String toString() {
    return "GetConnectedSynchronizersResponse{"
        + "connectedSynchronizers="
        + connectedSynchronizers
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetConnectedSynchronizersResponse that = (GetConnectedSynchronizersResponse) o;
    return Objects.equals(connectedSynchronizers, that.connectedSynchronizers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectedSynchronizers);
  }
}
