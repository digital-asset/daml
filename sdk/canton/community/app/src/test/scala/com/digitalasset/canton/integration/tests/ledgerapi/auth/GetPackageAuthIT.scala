// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  ListPackagesRequest,
  PackageServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetPackageAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PackageService#GetPackage"

  private def loadPackages(token: Option[String]) =
    stub(PackageServiceGrpc.stub(channel), token)
      .listPackages(ListPackagesRequest())

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*

    for {
      loadPackageResponse <- loadPackages(context.token)
      _ <- stub(PackageServiceGrpc.stub(channel), context.token)
        .getPackage(GetPackageRequest(packageId = loadPackageResponse.packageIds.head))
    } yield ()
  }

}
