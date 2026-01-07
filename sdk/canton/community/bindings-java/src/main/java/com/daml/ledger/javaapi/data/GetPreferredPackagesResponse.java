// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GetPreferredPackagesResponse {
  @NonNull List<@NonNull PackageReference> packageReferences;
  @NonNull String synchronizerId;

  public GetPreferredPackagesResponse(
      @NonNull List<@NonNull PackageReference> packageReferences, @NonNull String synchronizerId) {
    this.packageReferences = packageReferences;
    this.synchronizerId = synchronizerId;
  }

  @NonNull
  public List<@NonNull PackageReference> getPackageReferences() {
    return packageReferences;
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  public static GetPreferredPackagesResponse fromProto(
      InteractiveSubmissionServiceOuterClass.@NonNull GetPreferredPackagesResponse
          getPreferredPackagesResponse) {

    return new GetPreferredPackagesResponse(
        getPreferredPackagesResponse.getPackageReferencesList().stream()
            .map(PackageReference::fromProto)
            .toList(),
        getPreferredPackagesResponse.getSynchronizerId());
  }

  public InteractiveSubmissionServiceOuterClass.@NonNull GetPreferredPackagesResponse toProto() {
    return InteractiveSubmissionServiceOuterClass.GetPreferredPackagesResponse.newBuilder()
        .addAllPackageReferences(
            packageReferences.stream().map(PackageReference::toProto).collect(Collectors.toList()))
        .setSynchronizerId(synchronizerId)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    GetPreferredPackagesResponse that = (GetPreferredPackagesResponse) o;
    return Objects.equals(packageReferences, that.packageReferences)
        && Objects.equals(synchronizerId, that.synchronizerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageReferences, synchronizerId);
  }

  public @NonNull String toString() {
    return "GetPreferredPackagesResponse{"
        + "packageReferences="
        + packageReferences
        + ", synchronizerId="
        + synchronizerId
        + '}';
  }
}
