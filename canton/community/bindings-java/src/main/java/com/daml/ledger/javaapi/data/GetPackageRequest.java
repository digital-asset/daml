// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.PackageServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetPackageRequest {
  private final @NonNull String packageId;

  public GetPackageRequest(@NonNull String packageId) {
    this.packageId = packageId;
  }

  @NonNull
  String getPackageId() {
    return packageId;
  }

  public static GetPackageRequest fromProto(PackageServiceOuterClass.GetPackageRequest request) {
    return new GetPackageRequest(request.getPackageId());
  }

  public PackageServiceOuterClass.GetPackageRequest toProto() {
    return PackageServiceOuterClass.GetPackageRequest.newBuilder().setPackageId(packageId).build();
  }

  @Override
  public String toString() {
    return "GetPackageRequest{" + "packageId='" + packageId + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetPackageRequest that = (GetPackageRequest) o;
    return Objects.equals(packageId, that.packageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId);
  }
}
