// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetLatestPrunedOffsetsResponse {

  @NonNull private final Long participantPrunedUpToInclusive;
  @NonNull private final Long allDivulgedContractsPrunedUpToInclusive;

  public GetLatestPrunedOffsetsResponse(
      @NonNull Long participantPrunedUpToInclusive,
      @NonNull Long allDivulgedContractsPrunedUpToInclusive) {
    this.participantPrunedUpToInclusive = participantPrunedUpToInclusive;
    this.allDivulgedContractsPrunedUpToInclusive = allDivulgedContractsPrunedUpToInclusive;
  }

  @NonNull
  public Long getParticipantPrunedUpToInclusive() {
    return participantPrunedUpToInclusive;
  }

  @NonNull
  public Long getAllDivulgedContractsPrunedUpToInclusive() {
    return allDivulgedContractsPrunedUpToInclusive;
  }

  public static GetLatestPrunedOffsetsResponse fromProto(
      StateServiceOuterClass.GetLatestPrunedOffsetsResponse response) {
    return new GetLatestPrunedOffsetsResponse(
        response.getParticipantPrunedUpToInclusive(),
        response.getAllDivulgedContractsPrunedUpToInclusive());
  }

  public StateServiceOuterClass.GetLatestPrunedOffsetsResponse toProto() {
    return StateServiceOuterClass.GetLatestPrunedOffsetsResponse.newBuilder()
        .setParticipantPrunedUpToInclusive(participantPrunedUpToInclusive)
        .setAllDivulgedContractsPrunedUpToInclusive(allDivulgedContractsPrunedUpToInclusive)
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
