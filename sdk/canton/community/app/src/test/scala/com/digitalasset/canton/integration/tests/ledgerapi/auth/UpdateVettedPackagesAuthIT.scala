// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.package_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.ledger.api.PriorTopologySerialExists

import scala.concurrent.Future

final class UpdateVettedPackagesAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PackageManagementService#UpdateVettedPackages"

  lazy private val request =
    UpdateVettedPackagesRequest(
      Seq(),
      false,
      "",
      Some(PriorTopologySerialExists(0).toProtoLAPI),
    )

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token).updateVettedPackages(request)
}
