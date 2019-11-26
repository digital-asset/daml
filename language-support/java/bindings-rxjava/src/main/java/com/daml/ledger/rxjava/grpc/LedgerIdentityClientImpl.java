// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.LedgerIdentityClient;
import com.digitalasset.ledger.api.auth.client.LedgerCallCredentials;
import com.digitalasset.ledger.api.v1.LedgerIdentityServiceGrpc;
import com.digitalasset.ledger.api.v1.LedgerIdentityServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Single;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Optional;

public class LedgerIdentityClientImpl implements LedgerIdentityClient {

    private LedgerIdentityServiceGrpc.LedgerIdentityServiceFutureStub serviceStub;

    public LedgerIdentityClientImpl(Channel channel) {
        this.serviceStub = LedgerIdentityServiceGrpc.newFutureStub(channel);
    }

    public LedgerIdentityClientImpl(@NonNull Channel channel, @NonNull String accessToken) {
        this(channel);
        this.serviceStub = this.serviceStub.withCallCredentials(new LedgerCallCredentials(accessToken));
    }

    private Single<String> getLedgerIdentity(@NonNull Optional<String> accessToken) {
        LedgerIdentityServiceGrpc.LedgerIdentityServiceFutureStub stub = this.serviceStub;
        accessToken.ifPresent(t -> this.serviceStub.withCallCredentials(new LedgerCallCredentials(t)));
        return Single
                .fromFuture(stub.getLedgerIdentity(LedgerIdentityServiceOuterClass.GetLedgerIdentityRequest.getDefaultInstance()))
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
