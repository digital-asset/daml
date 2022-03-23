// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc;

// Examples writen natively in JAVA -- using static methods
class ByHandJava {

    // v1: uppercase "Long" -- slow!
    static Long nfib_v1(Long n) {
        if (n < 2) {
            return 1L;
        } else {
            return nfib_v1(n - 1) + nfib_v1(n - 2) + 1L;
        }
    }

    // v2: lowercase "long" -- quick!
    static long nfib_v2(long n) {
        if (n < 2) {
            return 1L;
        } else {
            return nfib_v2(n - 1) + nfib_v2(n - 2) + 1L;
        }
    }


}
