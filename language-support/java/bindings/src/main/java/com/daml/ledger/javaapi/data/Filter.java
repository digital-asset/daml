// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.TransactionFilterOuterClass;

public abstract class Filter {

    public static Filter fromProto(TransactionFilterOuterClass.Filters filters) {
        if (filters.hasInclusive()) {
            return InclusiveFilter.fromProto(filters.getInclusive());
        } else {
            return NoFilter.instance;
        }
    }

    public abstract TransactionFilterOuterClass.Filters toProto();
}
