// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc;

import com.daml.ledger.rxjava.PackageClient;
import com.daml.ledger.javaapi.data.GetPackageResponse;
import com.daml.ledger.javaapi.data.GetPackageStatusResponse;
import com.digitalasset.ledger.api.v1.PackageServiceGrpc;
import com.digitalasset.ledger.api.v1.PackageServiceOuterClass;
import io.grpc.Channel;
import io.reactivex.Flowable;
import io.reactivex.Single;

public class PackageClientImpl implements PackageClient {

    private final String ledgerId;
    private final PackageServiceGrpc.PackageServiceFutureStub serviceStub;

    public PackageClientImpl(String ledgerId, Channel channel) {
        this.ledgerId = ledgerId;
        serviceStub = PackageServiceGrpc.newFutureStub(channel);
    }

    @Override
    public Flowable<String> listPackages() {
        PackageServiceOuterClass.ListPackagesRequest request = PackageServiceOuterClass.ListPackagesRequest.newBuilder().setLedgerId(ledgerId).build();
        return Flowable
                .fromFuture(serviceStub.listPackages(request))
                .concatMapIterable(PackageServiceOuterClass.ListPackagesResponse::getPackageIdsList);
    }

    @Override
    public Single<GetPackageResponse> getPackage(String packageId) {
        PackageServiceOuterClass.GetPackageRequest request = PackageServiceOuterClass.GetPackageRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setPackageId(packageId)
                .build();
        return Single.fromFuture(serviceStub.getPackage(request)).map(GetPackageResponse::fromProto);
    }

    @Override
    public Single<GetPackageStatusResponse> getPackageStatus(String packageId) {
        PackageServiceOuterClass.GetPackageStatusRequest request = PackageServiceOuterClass.GetPackageStatusRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setPackageId(packageId)
                .build();
        return Single
                .fromFuture(serviceStub.getPackageStatus(request))
                .map(GetPackageStatusResponse::fromProto);
    }
}
