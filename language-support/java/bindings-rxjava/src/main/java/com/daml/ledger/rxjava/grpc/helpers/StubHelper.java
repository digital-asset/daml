// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers;

import com.digitalasset.ledger.api.auth.client.LedgerCallCredentials;
import io.grpc.stub.AbstractStub;

import java.util.Optional;

public final class StubHelper {

    // This is intended exclusively as an helper module
    private StubHelper() {}

    public static <T extends AbstractStub<T>> T authenticating(T stub, Optional<String> maybeToken) {
        T t = stub;
        if (maybeToken.isPresent()) {
            t = LedgerCallCredentials.authenticatingStub(t, maybeToken.get());
        }
        return t;
    }

}
