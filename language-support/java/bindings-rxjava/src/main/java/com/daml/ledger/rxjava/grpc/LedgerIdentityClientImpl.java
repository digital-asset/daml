// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.api.v1.LedgerIdentityServiceGrpc;
import com.daml.ledger.api.v1.LedgerIdentityServiceOuterClass;
import com.daml.ledger.rxjava.LedgerIdentityClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import io.grpc.Channel;
import io.reactivex.Single;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

public class LedgerIdentityClientImpl implements LedgerIdentityClient {

  private LedgerIdentityServiceGrpc.LedgerIdentityServiceFutureStub serviceStub;

  public LedgerIdentityClientImpl(Channel channel, Optional<String> accessToken) {
    this.serviceStub =
        StubHelper.authenticating(LedgerIdentityServiceGrpc.newFutureStub(channel), accessToken);
  }

  private Single<String> getLedgerIdentity(@NonNull Optional<String> accessToken) {
    return Single.fromFuture(
            StubHelper.authenticating(this.serviceStub, accessToken)
                .getLedgerIdentity(
                    LedgerIdentityServiceOuterClass.GetLedgerIdentityRequest.getDefaultInstance()))
        .map(LedgerIdentityServiceOuterClass.GetLedgerIdentityResponse::getLedgerId);
  }

  @Override
  public Single<String> getLedgerIdentity() {
    return getLedgerIdentity(Optional.empty());
  }

  @Override
  public Single<String> getLedgerIdentity(@NonNull String accessToken) {
    return getLedgerIdentity(Optional.of(accessToken));
  }
}
