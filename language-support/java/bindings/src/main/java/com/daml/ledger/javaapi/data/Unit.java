// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;
import com.google.protobuf.Empty;

public class Unit extends Value {

    static private Unit instance = new Unit();

    private Unit() {
    }

    static public Unit getInstance() {
        return Unit.instance;
    }

    @Override
    public ValueOuterClass.Value toProto() {
        Empty empty = Empty.newBuilder().build();
        ValueOuterClass.Value value = ValueOuterClass.Value.newBuilder().setUnit(empty).build();
        return value;
    }

    @Override
    public String toString() {
        return "Unit{}";
    }
}
