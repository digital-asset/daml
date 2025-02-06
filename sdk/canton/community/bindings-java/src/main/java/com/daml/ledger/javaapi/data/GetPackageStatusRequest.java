// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.PackageServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;

public final class GetPackageStatusRequest {
  private final @NonNull String packageId;

  public GetPackageStatusRequest(@NonNull String packageId) {
    this.packageId = packageId;
  }

  @NonNull
  String getPackageId() {
    return packageId;
  }

  public static GetPackageStatusRequest fromProto(
      PackageServiceOuterClass.GetPackageStatusRequest request) {
    return new GetPackageStatusRequest(request.getPackageId());
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
    GetPackageStatusRequest that = (GetPackageStatusRequest) o;
    return Objects.equals(packageId, that.packageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packageId);
  }
}
