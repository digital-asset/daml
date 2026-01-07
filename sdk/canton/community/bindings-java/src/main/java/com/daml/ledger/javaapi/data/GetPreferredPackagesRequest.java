// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.*;

public final class GetPreferredPackagesRequest {
  @NonNull private final List<@NonNull PackageVettingRequirement> packageVettingRequirements;
  @NonNull private final Optional<String> synchronizerId;
  @NonNull private final Optional<Instant> vettingValidAt;

  public GetPreferredPackagesRequest(
      @NonNull List<@NonNull PackageVettingRequirement> packageVettingRequirements,
      @NonNull Optional<String> synchronizerId,
      @NonNull Optional<Instant> vettingValidAt) {
    this.packageVettingRequirements = packageVettingRequirements;
    this.synchronizerId = synchronizerId;
    this.vettingValidAt = vettingValidAt;
  }

  public static GetPreferredPackagesRequest fromProto(
      InteractiveSubmissionServiceOuterClass.GetPreferredPackagesRequest request) {
    return new GetPreferredPackagesRequest(
        request.getPackageVettingRequirementsList().stream()
            .map(PackageVettingRequirement::fromProto)
            .toList(),
        Optional.of(request.getSynchronizerId()).filter(s -> !s.isEmpty()),
        request.hasVettingValidAt()
            ? Optional.of(
                Instant.ofEpochSecond(
                    request.getVettingValidAt().getSeconds(),
                    request.getVettingValidAt().getNanos()))
            : Optional.empty());
  }

  public InteractiveSubmissionServiceOuterClass.GetPreferredPackagesRequest toProto() {
    InteractiveSubmissionServiceOuterClass.GetPreferredPackagesRequest.Builder builder =
        InteractiveSubmissionServiceOuterClass.GetPreferredPackagesRequest.newBuilder();
    packageVettingRequirements.forEach(
        packageVettingRequirements ->
            builder.addPackageVettingRequirements(packageVettingRequirements.toProto()));
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

  public @NonNull List<@NonNull PackageVettingRequirement> getPackageVettingRequirements() {
    return packageVettingRequirements;
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
    GetPreferredPackagesRequest that = (GetPreferredPackagesRequest) o;
    return Objects.equals(packageVettingRequirements, that.packageVettingRequirements)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(vettingValidAt, that.vettingValidAt);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(packageVettingRequirements, synchronizerId, vettingValidAt);
  }

  @Override
  public String toString() {
    return "GetPreferredPackagesRequest{"
        + "packageVettingRequirements="
        + packageVettingRequirements
        + ", synchronizerId="
        + synchronizerId
        + ", vettingValidAt="
        + vettingValidAt
        + '}';
  }
}
