// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetLatestPrunedOffsetsResponse {

  @NonNull private final Optional<Long> participantPrunedUpToInclusive;
  @NonNull private final Optional<Long> allDivulgedContractsPrunedUpToInclusive;

  public GetLatestPrunedOffsetsResponse(
      @NonNull Optional<Long> participantPrunedUpToInclusive,
      @NonNull Optional<Long> allDivulgedContractsPrunedUpToInclusive) {
    this.participantPrunedUpToInclusive = participantPrunedUpToInclusive;
    this.allDivulgedContractsPrunedUpToInclusive = allDivulgedContractsPrunedUpToInclusive;
  }

  @NonNull
  public Optional<Long> getParticipantPrunedUpToInclusive() {
    return participantPrunedUpToInclusive;
  }

  @NonNull
  public Optional<Long> getAllDivulgedContractsPrunedUpToInclusive() {
    return allDivulgedContractsPrunedUpToInclusive;
  }

  public static GetLatestPrunedOffsetsResponse fromProto(
      StateServiceOuterClass.GetLatestPrunedOffsetsResponse response) {
    return new GetLatestPrunedOffsetsResponse(
        response.hasParticipantPrunedUpToInclusive()
            ? Optional.of(response.getParticipantPrunedUpToInclusive())
            : Optional.empty(),
        response.hasAllDivulgedContractsPrunedUpToInclusive()
            ? Optional.of(response.getAllDivulgedContractsPrunedUpToInclusive())
            : Optional.empty());
  }

  public StateServiceOuterClass.GetLatestPrunedOffsetsResponse toProto() {
    StateServiceOuterClass.GetLatestPrunedOffsetsResponse.Builder builder =
        StateServiceOuterClass.GetLatestPrunedOffsetsResponse.newBuilder();
    participantPrunedUpToInclusive.ifPresent(builder::setParticipantPrunedUpToInclusive);
    allDivulgedContractsPrunedUpToInclusive.ifPresent(
        builder::setAllDivulgedContractsPrunedUpToInclusive);
    return builder.build();
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
