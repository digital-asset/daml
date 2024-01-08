// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetLatestPrunedOffsetsResponseV2 {

  @NonNull private final ParticipantOffsetV2 participantPrunedUpToInclusive;
  @NonNull private final ParticipantOffsetV2 allDivulgedContractsPrunedUpToInclusive;

  public GetLatestPrunedOffsetsResponseV2(
      @NonNull ParticipantOffsetV2 participantPrunedUpToInclusive,
      @NonNull ParticipantOffsetV2 allDivulgedContractsPrunedUpToInclusive) {
    this.participantPrunedUpToInclusive = participantPrunedUpToInclusive;
    this.allDivulgedContractsPrunedUpToInclusive = allDivulgedContractsPrunedUpToInclusive;
  }

  @NonNull
  public ParticipantOffsetV2 getParticipantPrunedUpToInclusive() {
    return participantPrunedUpToInclusive;
  }

  @NonNull
  public ParticipantOffsetV2 getAllDivulgedContractsPrunedUpToInclusive() {
    return allDivulgedContractsPrunedUpToInclusive;
  }

  public static GetLatestPrunedOffsetsResponseV2 fromProto(
      StateServiceOuterClass.GetLatestPrunedOffsetsResponse response) {
    return new GetLatestPrunedOffsetsResponseV2(
        ParticipantOffsetV2.fromProto(response.getParticipantPrunedUpToInclusive()),
        ParticipantOffsetV2.fromProto(response.getAllDivulgedContractsPrunedUpToInclusive()));
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
    GetLatestPrunedOffsetsResponseV2 that = (GetLatestPrunedOffsetsResponseV2) o;
    return Objects.equals(participantPrunedUpToInclusive, that.participantPrunedUpToInclusive)
        && Objects.equals(
            allDivulgedContractsPrunedUpToInclusive, that.allDivulgedContractsPrunedUpToInclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(participantPrunedUpToInclusive, allDivulgedContractsPrunedUpToInclusive);
  }
}
