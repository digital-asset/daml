// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.javaapi.data.GetPackageResponse;
import com.daml.ledger.javaapi.data.GetPackageStatusResponse;
import com.daml.ledger.rxjava.PackageClient;
import com.daml.ledger.rxjava.grpc.helpers.StubHelper;
import com.daml.ledger.api.v1.PackageServiceGrpc;
import com.daml.ledger.api.v1.PackageServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.util.Optional;

public class PackageClientImpl implements PackageClient {

    private final String ledgerId;
    private final PackageServiceGrpc.PackageServiceFutureStub serviceStub;

    public PackageClientImpl(String ledgerId, Channel channel, Optional<String> accessToken) {
        this.ledgerId = ledgerId;
        serviceStub = StubHelper.authenticating(PackageServiceGrpc.newFutureStub(channel), accessToken);
    }

    private Flowable<String> listPackages(Optional<String> accessToken) {
        PackageServiceOuterClass.ListPackagesRequest request = PackageServiceOuterClass.ListPackagesRequest.newBuilder().setLedgerId(ledgerId).build();
        return Flowable
                .fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).listPackages(request))
                .concatMapIterable(PackageServiceOuterClass.ListPackagesResponse::getPackageIdsList);
    }

    @Override
    public Flowable<String> listPackages() {
        return listPackages(Optional.empty());
    }

    @Override
    public Flowable<String> listPackages(String accessToken) {
        return listPackages(Optional.of(accessToken));
    }

    private Single<GetPackageResponse> getPackage(String packageId, Optional<String> accessToken) {
        PackageServiceOuterClass.GetPackageRequest request = PackageServiceOuterClass.GetPackageRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setPackageId(packageId)
                .build();
        return Single.fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).getPackage(request)).map(GetPackageResponse::fromProto);
    }

    @Override
    public Single<GetPackageResponse> getPackage(String packageId) {
        return getPackage(packageId, Optional.empty());
    }

    @Override
    public Single<GetPackageResponse> getPackage(String packageId, String accessToken) {
        return getPackage(packageId, Optional.of(accessToken));
    }

    public Single<GetPackageStatusResponse> getPackageStatus(String packageId, Optional<String> accessToken) {
        PackageServiceOuterClass.GetPackageStatusRequest request = PackageServiceOuterClass.GetPackageStatusRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setPackageId(packageId)
                .build();
        return Single
                .fromFuture(StubHelper.authenticating(this.serviceStub, accessToken).getPackageStatus(request))
                .map(GetPackageStatusResponse::fromProto);
    }

    @Override
    public Single<GetPackageStatusResponse> getPackageStatus(String packageId) {
        return getPackageStatus(packageId, Optional.empty());
    }

    @Override
    public Single<GetPackageStatusResponse> getPackageStatus(String packageId, String accessToken) {
        return getPackageStatus(packageId, Optional.of(accessToken));
    }
}
