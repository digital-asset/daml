// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers;

import com.google.protobuf.Timestamp;

public class TimestampComparator {

    public static int compare(Timestamp t1, Timestamp t2) {
        int secondsComparison = Long.compare(t1.getSeconds(), t2.getSeconds());
        if (secondsComparison != 0) {
            return secondsComparison;
        } else {
            return Integer.compare(t1.getNanos(), t2.getNanos());
        }
    }
}
