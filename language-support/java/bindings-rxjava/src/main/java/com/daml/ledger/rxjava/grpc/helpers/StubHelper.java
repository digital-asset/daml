// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers;

import com.daml.ledger.api.auth.client.LedgerCallCredentials;
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
