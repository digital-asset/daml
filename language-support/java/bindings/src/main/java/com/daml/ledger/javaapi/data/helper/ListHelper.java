// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.helper;

import java.util.ArrayList;
import java.util.List;

public final class ListHelper {
    private ListHelper() {}
    public static <T> List<T> list(T... args) {
        List<T> ret = new ArrayList<>();
        for(T arg : args){
            ret.add(arg);
        }
        return ret;
    }
}

