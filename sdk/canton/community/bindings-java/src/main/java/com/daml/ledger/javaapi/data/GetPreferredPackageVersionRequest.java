// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.*;

public final class GetPreferredPackageVersionRequest {
  @NonNull private final List<@NonNull String> parties;
  @NonNull private final String packageName;
  @NonNull private final Optional<String> synchronizerId;
  @NonNull private final Optional<Instant> vettingValidAt;

  public GetPreferredPackageVersionRequest(
      @NonNull List<@NonNull String> parties,
      @NonNull String packageName,
      @NonNull Optional<String> synchronizerId,
      @NonNull Optional<Instant> vettingValidAt) {
    this.parties = parties;
    this.packageName = packageName;
    this.synchronizerId = synchronizerId;
    this.vettingValidAt = vettingValidAt;
  }

  public static GetPreferredPackageVersionRequest fromProto(
      InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionRequest request) {
    return new GetPreferredPackageVersionRequest(
        request.getPartiesList(),
        request.getPackageName(),
        Optional.of(request.getSynchronizerId()).filter(s -> !s.isEmpty()),
        request.hasVettingValidAt()
            ? Optional.of(
                Instant.ofEpochSecond(
                    request.getVettingValidAt().getSeconds(),
                    request.getVettingValidAt().getNanos()))
            : Optional.empty());
  }

  public InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionRequest toProto() {
    InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionRequest.Builder builder =
        InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionRequest.newBuilder();
    builder.addAllParties(parties);
    builder.setPackageName(packageName);
    synchronizerId.ifPresent(builder::setSynchronizerId);
    vettingValidAt.ifPresent(
        instant ->
            builder.setVettingValidAt(
                com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(instant.getEpochSecond())
                    .setNanos(instant.getNano())
                    .build()));
    return builder.build();
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  @NonNull
  public String getPackageName() {
    return packageName;
  }

  @NonNull
  public Optional<String> getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  public Optional<Instant> getVettingValidAt() {
    return vettingValidAt;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    GetPreferredPackageVersionRequest that = (GetPreferredPackageVersionRequest) o;
    return Objects.deepEquals(parties, that.parties)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(vettingValidAt, that.vettingValidAt);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(parties, packageName, synchronizerId, vettingValidAt);
  }

  public String toString() {
    return "GetPreferredPackageVersionRequest{"
        + "parties="
        + parties
        + ", packageName="
        + packageName
        + ", synchronizerId="
        + synchronizerId
        + ", vettingValidAt="
        + vettingValidAt
        + '}';
  }
}
