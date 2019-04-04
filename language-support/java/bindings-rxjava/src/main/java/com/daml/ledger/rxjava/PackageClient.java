// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

    Single<GetPackageResponse> getPackage(String packageId);

    Single<GetPackageStatusResponse> getPackageStatus(String packageId);
}
