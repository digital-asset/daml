// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.interactive.InteractiveSubmissionServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class PackagePreference {
  @NonNull PackageReference packageReference;
  @NonNull String synchronizerId;

  public PackagePreference(
      @NonNull PackageReference packageReference, @NonNull String synchronizerId) {
    this.packageReference = packageReference;
    this.synchronizerId = synchronizerId;
  }

  @NonNull
  public PackageReference getPackageReference() {
    return packageReference;
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  public static PackagePreference fromProto(
      InteractiveSubmissionServiceOuterClass.@NonNull PackagePreference packagePreference) {
    return new PackagePreference(
        PackageReference.fromProto(packagePreference.getPackageReference()),
        packagePreference.getSynchronizerId());
  }

  public InteractiveSubmissionServiceOuterClass.@NonNull PackagePreference toProto() {
    return InteractiveSubmissionServiceOuterClass.PackagePreference.newBuilder()
        .setPackageReference(packageReference.toProto())
        .setSynchronizerId(synchronizerId)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    PackagePreference that = (PackagePreference) o;
    return Objects.equals(packageReference, that.packageReference)
        && Objects.equals(synchronizerId, that.synchronizerId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageReference, synchronizerId);
  }
}
