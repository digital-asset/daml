// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      loadPackageResponse <- loadPackages(context.token)
      _ <- stub(PackageServiceGrpc.stub(channel), context.token)
        .getPackage(GetPackageRequest(packageId = loadPackageResponse.packageIds.head))
    } yield ()

}
