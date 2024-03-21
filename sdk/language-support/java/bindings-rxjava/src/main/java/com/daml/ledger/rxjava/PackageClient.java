// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

import com.daml.ledger.javaapi.data.GetPackageResponse;
import com.daml.ledger.javaapi.data.GetPackageStatusResponse;
import io.reactivex.Flowable;
import io.reactivex.Single;

/** An RxJava version of {@link com.daml.ledger.api.v1.PackageServiceGrpc} */
public interface PackageClient {
  Flowable<String> listPackages();

  Flowable<String> listPackages(String accessToken);

  Single<GetPackageResponse> getPackage(String packageId);

  Single<GetPackageResponse> getPackage(String packageId, String accessToken);

  Single<GetPackageStatusResponse> getPackageStatus(String packageId);

  Single<GetPackageStatusResponse> getPackageStatus(String packageId, String accessToken);
}
