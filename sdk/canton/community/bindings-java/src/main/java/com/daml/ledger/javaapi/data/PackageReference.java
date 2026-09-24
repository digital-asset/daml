// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.PackageReferenceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class PackageReference {
  @NonNull private final String packageId;
  @NonNull private final String packageName;
  @NonNull private final PackageVersion packageVersion;

  public PackageReference(
      @NonNull String packageId,
      @NonNull String packageName,
      @NonNull PackageVersion packageVersion) {
    this.packageId = packageId;
    this.packageName = packageName;
    this.packageVersion = packageVersion;
  }

  @NonNull
  public String getPackageId() {
    return packageId;
  }

  @NonNull
  public String getPackageName() {
    return packageName;
  }

  @NonNull
  public PackageVersion getPackageVersion() {
    return packageVersion;
  }

  @NonNull
  public static PackageReference fromProto(
      PackageReferenceOuterClass.@NonNull PackageReference packageReference) {
    return new PackageReference(
        packageReference.getPackageId(),
        packageReference.getPackageName(),
        PackageVersion.unsafeFromString(packageReference.getPackageVersion()));
  }

  public PackageReferenceOuterClass.@NonNull PackageReference toProto() {
    return PackageReferenceOuterClass.PackageReference.newBuilder()
        .setPackageId(packageId)
        .setPackageName(packageName)
        .setPackageVersion(packageVersion.toString())
        .build();
  }

  @Override
  public String toString() {
    return "PackageReference{"
        + "packageId='"
        + packageId
        + '\''
        + ", packageName='"
        + packageName
        + '\''
        + ", packageVersion="
        + packageVersion
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    PackageReference that = (PackageReference) o;
    return Objects.equals(packageId, that.packageId)
        && Objects.equals(packageName, that.packageName)
        && Objects.equals(packageVersion, that.packageVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId, packageName, packageVersion);
  }
}
