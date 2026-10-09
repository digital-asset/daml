// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetPreferredPackageVersionResponse {
  @NonNull private final Optional<PackagePreference> packagePreference;

  @NonNull
  public GetPreferredPackageVersionResponse(
      @NonNull Optional<PackagePreference> packagePreference) {
    this.packagePreference = packagePreference;
  }

  @NonNull
  public Optional<PackagePreference> getPackagePreference() {
    return packagePreference;
  }

  @NonNull
  public static GetPreferredPackageVersionResponse fromProto(
      InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionResponse response) {
    return new GetPreferredPackageVersionResponse(
        response.hasPackagePreference()
            ? Optional.of(PackagePreference.fromProto(response.getPackagePreference()))
            : Optional.empty());
  }

  public InteractiveSubmissionServiceOuterClass.@NonNull GetPreferredPackageVersionResponse
      toProto() {
    InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionResponse.Builder builder =
        InteractiveSubmissionServiceOuterClass.GetPreferredPackageVersionResponse.newBuilder();
    packagePreference.ifPresent(p -> builder.setPackagePreference(p.toProto()));
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    GetPreferredPackageVersionResponse that = (GetPreferredPackageVersionResponse) o;
    return Objects.equals(packagePreference, that.packagePreference);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(packagePreference);
  }

  public String toString() {
    return "GetPreferredPackageVersionResponse{" + "packagePreference=" + packagePreference + '}';
  }
}
