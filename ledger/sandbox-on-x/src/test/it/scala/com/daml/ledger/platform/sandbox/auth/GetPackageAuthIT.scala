// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.package_service.{
  GetPackageRequest,
  ListPackagesRequest,
  PackageServiceGrpc,
}

import scala.concurrent.Future

final class GetPackageAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "PackageService#GetPackage"

  private def loadPackages(token: Option[String]) = {
    stub(PackageServiceGrpc.stub(channel), token)
      .listPackages(ListPackagesRequest())
  }

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    for {
      loadPackageResponse <- loadPackages(token)
      _ <- stub(PackageServiceGrpc.stub(channel), token)
        .getPackage(GetPackageRequest(packageId = loadPackageResponse.packageIds.head))
    } yield ()

}
