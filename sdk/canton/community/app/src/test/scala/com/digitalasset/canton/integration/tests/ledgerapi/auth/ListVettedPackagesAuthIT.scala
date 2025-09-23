// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.package_service.{
  ListVettedPackagesRequest,
  PackageMetadataFilter,
  PackageServiceGrpc,
  TopologyStateFilter,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

import scala.concurrent.Future

final class ListVettedPackagesAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PackageService#ListVettedPackages"

  private val request =
    ListVettedPackagesRequest(
      Some(PackageMetadataFilter(Seq(), Seq())),
      Some(TopologyStateFilter(Seq(), Seq())),
      None,
      None,
    )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageServiceGrpc.stub(channel), context.token).listVettedPackages(request)

}
