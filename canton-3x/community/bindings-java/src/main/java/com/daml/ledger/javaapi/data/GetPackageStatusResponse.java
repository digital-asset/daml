// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GetPackageStatusResponse {

  // Clone of the PackageServiceOuterClass.PackageStatus enumeration
  public enum PackageStatus {
    UNKNOWN(0),
    REGISTERED(1),
    UNRECOGNIZED(-1),
    ;

    private final int value;

    private static Map<Integer, PackageStatus> valueToEnumMap =
        EnumSet.allOf(PackageStatus.class).stream()
            .collect(Collectors.toMap(e -> e.value, Function.identity()));

    private PackageStatus(int value) {
      this.value = value;
    }

    public static PackageStatus valueOf(int value) {
      return valueToEnumMap.getOrDefault(value, UNRECOGNIZED);
    }
  }

  private final PackageStatus packageStatus;

  public GetPackageStatusResponse(PackageStatus packageStatus) {
    this.packageStatus = packageStatus;
  }

  public PackageStatus getPackageStatusValue() {
    return packageStatus;
  }

  public static GetPackageStatusResponse fromProto(
      com.daml.ledger.api.v1.PackageServiceOuterClass.GetPackageStatusResponse p) {
    return new GetPackageStatusResponse(PackageStatus.valueOf(p.getPackageStatusValue()));
  }
}
