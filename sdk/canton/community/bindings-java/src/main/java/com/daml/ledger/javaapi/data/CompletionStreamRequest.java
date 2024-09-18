// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class CompletionStreamRequest {

  @NonNull private final String applicationId;

  @NonNull private final List<@NonNull String> parties;

  @NonNull private final Optional<Long> beginExclusive;

  public CompletionStreamRequest(
      @NonNull String applicationId,
      @NonNull List<@NonNull String> parties,
      @NonNull Optional<Long> beginExclusive) {
    this.applicationId = applicationId;
    this.parties = List.copyOf(parties);
    this.beginExclusive = beginExclusive;
  }

  @NonNull
  public String getApplicationId() {
    return applicationId;
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  public Optional<Long> getBeginExclusive() {
    return beginExclusive;
  }

  public static CompletionStreamRequest fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamRequest request) {
    return new CompletionStreamRequest(
        request.getApplicationId(),
        request.getPartiesList(),
        request.hasBeginExclusive() ? Optional.of(request.getBeginExclusive()) : Optional.empty());
  }

  public CommandCompletionServiceOuterClass.CompletionStreamRequest toProto() {
    CommandCompletionServiceOuterClass.CompletionStreamRequest.Builder builder =
        CommandCompletionServiceOuterClass.CompletionStreamRequest.newBuilder()
            .setApplicationId(applicationId)
            .addAllParties(parties);

    beginExclusive.ifPresent(builder::setBeginExclusive);

    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompletionStreamRequest that = (CompletionStreamRequest) o;
    return Objects.equals(applicationId, that.applicationId)
        && Objects.equals(parties, that.parties)
        && Objects.equals(beginExclusive, that.beginExclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(applicationId, parties, beginExclusive);
  }

  @Override
  public String toString() {
    return "CompletionStreamRequest{"
        + "applicationId="
        + applicationId
        + ", parties="
        + parties
        + ", beginExclusive="
        + beginExclusive
        + '}';
  }
}
