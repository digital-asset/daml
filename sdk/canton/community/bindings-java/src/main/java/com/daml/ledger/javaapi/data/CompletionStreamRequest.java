// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandCompletionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class CompletionStreamRequest {

  @NonNull private final String userId;

  @NonNull private final List<@NonNull String> parties;

  @NonNull private final Long beginExclusive;

  public CompletionStreamRequest(
      @NonNull String userId,
      @NonNull List<@NonNull String> parties,
      @NonNull Long beginExclusive) {
    this.userId = userId;
    this.parties = List.copyOf(parties);
    this.beginExclusive = beginExclusive;
  }

  @NonNull
  public String getUserId() {
    return userId;
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  public Long getBeginExclusive() {
    return beginExclusive;
  }

  public static CompletionStreamRequest fromProto(
      CommandCompletionServiceOuterClass.CompletionStreamRequest request) {
    return new CompletionStreamRequest(
        request.getUserId(), request.getPartiesList(), request.getBeginExclusive());
  }

  public CommandCompletionServiceOuterClass.CompletionStreamRequest toProto() {
    CommandCompletionServiceOuterClass.CompletionStreamRequest.Builder builder =
        CommandCompletionServiceOuterClass.CompletionStreamRequest.newBuilder()
            .setUserId(userId)
            .addAllParties(parties)
            .setBeginExclusive(beginExclusive);

    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompletionStreamRequest that = (CompletionStreamRequest) o;
    return Objects.equals(userId, that.userId)
        && Objects.equals(parties, that.parties)
        && Objects.equals(beginExclusive, that.beginExclusive);
  }

  @Override
  public int hashCode() {

    return Objects.hash(userId, parties, beginExclusive);
  }

  @Override
  public String toString() {
    return "CompletionStreamRequest{"
        + "userId="
        + userId
        + ", parties="
        + parties
        + ", beginExclusive="
        + beginExclusive
        + '}';
  }
}
