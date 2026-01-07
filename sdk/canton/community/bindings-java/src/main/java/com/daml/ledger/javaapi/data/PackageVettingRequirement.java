// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;

public final class PackageVettingRequirement {
  @NonNull private final List<@NonNull String> parties;
  @NonNull private final String packageName;

  public PackageVettingRequirement(
      @NonNull List<@NonNull String> parties, @NonNull String packageName) {
    this.parties = parties;
    this.packageName = packageName;
  }

  public static PackageVettingRequirement fromProto(
      InteractiveSubmissionServiceOuterClass.PackageVettingRequirement proto) {
    return new PackageVettingRequirement(proto.getPartiesList(), proto.getPackageName());
  }

  public InteractiveSubmissionServiceOuterClass.PackageVettingRequirement toProto() {
    return InteractiveSubmissionServiceOuterClass.PackageVettingRequirement.newBuilder()
        .addAllParties(parties)
        .setPackageName(packageName)
        .build();
  }

  @NonNull
  public List<@NonNull String> getParties() {
    return parties;
  }

  @NonNull
  public String getPackageName() {
    return packageName;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    PackageVettingRequirement that = (PackageVettingRequirement) o;
    return Objects.equals(parties, that.parties) && Objects.equals(packageName, that.packageName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parties, packageName);
  }

  @Override
  public @NonNull String toString() {
    return "PackageVettingRequirement{"
        + "parties="
        + parties
        + ", packageName="
        + packageName
        + '}';
  }
}
