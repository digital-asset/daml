// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.pkg

import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.api.v1.package_service.*
import com.digitalasset.canton.ledger.api.domain.LedgerId

import scala.concurrent.Future

class PackageClient(val ledgerId: LedgerId, service: PackageServiceStub) {
  val it = new withoutledgerid.PackageClient(service)

  def listPackages(token: Option[String] = None): Future[ListPackagesResponse] =
    it.listPackages(ledgerId, token)

  def getPackage(packageId: String, token: Option[String] = None): Future[GetPackageResponse] =
    it.getPackage(packageId, ledgerId, token)

  def getPackageStatus(
      packageId: String,
      token: Option[String] = None,
  ): Future[GetPackageStatusResponse] = it.getPackageStatus(packageId, ledgerId, token)
}
