// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.TransactionFilterOuterClass;

public class NoFilter extends Filter {

    public static final NoFilter instance = new NoFilter();

    private NoFilter() {
    }

    @Override
    public TransactionFilterOuterClass.Filters toProto() {
        return TransactionFilterOuterClass.Filters.getDefaultInstance();
    }
}
