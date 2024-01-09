// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.PackageServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public final class GetPackageStatusRequestV2 {
  private final @NonNull String packageId;

  public GetPackageStatusRequestV2(@NonNull String packageId) {
    this.packageId = packageId;
  }

  @NonNull
  String getPackageId() {
    return packageId;
  }

  public static GetPackageStatusRequestV2 fromProto(
      PackageServiceOuterClass.GetPackageStatusRequest request) {
    return new GetPackageStatusRequestV2(request.getPackageId());
  }

  public PackageServiceOuterClass.GetPackageStatusRequest toProto() {
    return PackageServiceOuterClass.GetPackageStatusRequest.newBuilder()
        .setPackageId(packageId)
        .build();
  }

  @Override
  public String toString() {
    return "GetPackageStatusRequest{" + "packageId='" + packageId + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetPackageStatusRequestV2 that = (GetPackageStatusRequestV2) o;
    return Objects.equals(packageId, that.packageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId);
  }
}
