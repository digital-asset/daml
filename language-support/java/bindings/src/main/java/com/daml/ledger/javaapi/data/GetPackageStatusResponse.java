// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GetPackageStatusResponse {

    // Clone of the PackageServiceOuterClass.PackageStatus enumeration
    public enum PackageStatus {
        UNKNOWN(0),
        REGISTERED(1),
        UNRECOGNIZED(-1),
        ;

        private final int value;

        static private Map<Integer, PackageStatus> valueToEnumMap =
                EnumSet.allOf(PackageStatus.class).stream().collect(Collectors.toMap(e -> e.value, Function.identity()));

        private PackageStatus(int value) {
            this.value = value;
        }

        static public PackageStatus valueOf(int value) {
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

    public static GetPackageStatusResponse fromProto(com.digitalasset.ledger.api.v1.PackageServiceOuterClass.GetPackageStatusResponse p) {
        return new GetPackageStatusResponse(PackageStatus.valueOf(p.getPackageStatusValue()));
    }

}
