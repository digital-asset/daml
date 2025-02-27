// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.EventQueryServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

public final class GetEventsByContractIdRequest {

  @NonNull private final String contractId;
  @NonNull private final EventFormat eventFormat;

  public static GetEventsByContractIdRequest fromProto(
      EventQueryServiceOuterClass.GetEventsByContractIdRequest getEventsByContractIdRequest) {
    if (!getEventsByContractIdRequest.hasEventFormat())
      throw new IllegalArgumentException("eventFormat is mandatory");
    return new GetEventsByContractIdRequest(
        getEventsByContractIdRequest.getContractId(),
        EventFormat.fromProto(getEventsByContractIdRequest.getEventFormat()));
  }

  public EventQueryServiceOuterClass.GetEventsByContractIdRequest toProto() {
    return EventQueryServiceOuterClass.GetEventsByContractIdRequest.newBuilder()
        .setEventFormat(eventFormat.toProto())
        .setContractId(contractId)
        .build();
  }

  public String getContractId() {
    return contractId;
  }

  public EventFormat getEventFormat() {
    return eventFormat;
  }

  public GetEventsByContractIdRequest(
      @NonNull String contractId, @NonNull EventFormat eventFormat) {
    this.contractId = contractId;
    this.eventFormat = eventFormat;
  }

  @Override
  public String toString() {
    return "GetEventsByContractIdRequest{"
        + "contractId="
        + contractId
        + ", eventFormat="
        + eventFormat
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetEventsByContractIdRequest that = (GetEventsByContractIdRequest) o;
    return Objects.equals(contractId, that.contractId)
        && Objects.equals(eventFormat, that.eventFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(contractId, eventFormat);
  }
}
