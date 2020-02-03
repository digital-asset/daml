// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetPackageResponse;
import com.daml.ledger.javaapi.data.GetPackageStatusResponse;
import io.reactivex.Flowable;
import io.reactivex.Single;

/**
 * An RxJava version of {@link com.digitalasset.ledger.api.v1.PackageServiceGrpc}
 */
public interface PackageClient {
    Flowable<String> listPackages();
    Flowable<String> listPackages(String accessToken);

    Single<GetPackageResponse> getPackage(String packageId);
    Single<GetPackageResponse> getPackage(String packageId, String accessToken);

    Single<GetPackageStatusResponse> getPackageStatus(String packageId);
    Single<GetPackageStatusResponse> getPackageStatus(String packageId, String accessToken);
}
