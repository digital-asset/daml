// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetLatestPrunedOffsetsResponse {

  @NonNull private final ParticipantOffset participantPrunedUpToInclusive;
  @NonNull private final ParticipantOffset allDivulgedContractsPrunedUpToInclusive;

  public GetLatestPrunedOffsetsResponse(
      @NonNull ParticipantOffset participantPrunedUpToInclusive,
      @NonNull ParticipantOffset allDivulgedContractsPrunedUpToInclusive) {
    this.participantPrunedUpToInclusive = participantPrunedUpToInclusive;
    this.allDivulgedContractsPrunedUpToInclusive = allDivulgedContractsPrunedUpToInclusive;
  }

  @NonNull
  public ParticipantOffset getParticipantPrunedUpToInclusive() {
    return participantPrunedUpToInclusive;
  }

  @NonNull
  public ParticipantOffset getAllDivulgedContractsPrunedUpToInclusive() {
    return allDivulgedContractsPrunedUpToInclusive;
  }

  public static GetLatestPrunedOffsetsResponse fromProto(
      StateServiceOuterClass.GetLatestPrunedOffsetsResponse response) {
    return new GetLatestPrunedOffsetsResponse(
        ParticipantOffset.fromProto(response.getParticipantPrunedUpToInclusive()),
        ParticipantOffset.fromProto(response.getAllDivulgedContractsPrunedUpToInclusive()));
  }

  public StateServiceOuterClass.GetLatestPrunedOffsetsResponse toProto() {
    return StateServiceOuterClass.GetLatestPrunedOffsetsResponse.newBuilder()
        .setParticipantPrunedUpToInclusive(participantPrunedUpToInclusive.toProto())
        .setAllDivulgedContractsPrunedUpToInclusive(
            allDivulgedContractsPrunedUpToInclusive.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "GetLatestPrunedOffsetsResponse{"
        + "participantPrunedUpToInclusive="
        + participantPrunedUpToInclusive
        + ", allDivulgedContractsPrunedUpToInclusive="
        + allDivulgedContractsPrunedUpToInclusive
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetLatestPrunedOffsetsResponse that = (GetLatestPrunedOffsetsResponse) o;
    return Objects.equals(participantPrunedUpToInclusive, that.participantPrunedUpToInclusive)
        && Objects.equals(
            allDivulgedContractsPrunedUpToInclusive, that.allDivulgedContractsPrunedUpToInclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(participantPrunedUpToInclusive, allDivulgedContractsPrunedUpToInclusive);
  }
}
