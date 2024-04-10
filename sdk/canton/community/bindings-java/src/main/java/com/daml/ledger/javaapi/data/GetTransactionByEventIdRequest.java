// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetTransactionByEventIdRequest {

  @NonNull private final String eventId;

  @NonNull private final List<@NonNull String> requestingParties;

  public GetTransactionByEventIdRequest(
      @NonNull String eventId, @NonNull List<@NonNull String> requestingParties) {
    this.eventId = eventId;
    this.requestingParties = List.copyOf(requestingParties);
  }

  @NonNull
  public String getEventId() {
    return eventId;
  }

  @NonNull
  public List<@NonNull String> getRequestingParties() {
    return requestingParties;
  }

  public static GetTransactionByEventIdRequest fromProto(
      UpdateServiceOuterClass.GetTransactionByEventIdRequest request) {
    return new GetTransactionByEventIdRequest(
        request.getEventId(), request.getRequestingPartiesList());
  }

  public UpdateServiceOuterClass.GetTransactionByEventIdRequest toProto() {
    return UpdateServiceOuterClass.GetTransactionByEventIdRequest.newBuilder()
        .setEventId(eventId)
        .addAllRequestingParties(requestingParties)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTransactionByEventIdRequest that = (GetTransactionByEventIdRequest) o;
    return Objects.equals(eventId, that.eventId)
        && Objects.equals(requestingParties, that.requestingParties);
  }

  @Override
  public int hashCode() {

    return Objects.hash(eventId, requestingParties);
  }

  @Override
  public String toString() {
    return "GetTransactionByEventIdRequest{"
        + "eventId="
        + eventId
        + ", requestingParties="
        + requestingParties
        + '}';
  }
}
