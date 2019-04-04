// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.LedgerIdentityClient;
import com.digitalasset.ledger.api.v1.LedgerIdentityServiceGrpc;
import com.digitalasset.ledger.api.v1.LedgerIdentityServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Single;

public class LedgerIdentityClientImpl implements LedgerIdentityClient {

    private LedgerIdentityServiceGrpc.LedgerIdentityServiceFutureStub serviceStub;

    public LedgerIdentityClientImpl(Channel channel) {
        this.serviceStub = LedgerIdentityServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Single<String> getLedgerIdentity() {
        return Single
                .fromFuture(serviceStub.getLedgerIdentity(LedgerIdentityServiceOuterClass.GetLedgerIdentityRequest.getDefaultInstance()))
                .map(LedgerIdentityServiceOuterClass.GetLedgerIdentityResponse::getLedgerId);
    }
}
