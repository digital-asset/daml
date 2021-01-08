// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ListHelper {
    private ListHelper() {}
    public static <T> List<T> list(T... args) {
        return Arrays.asList(args);
    }
}

