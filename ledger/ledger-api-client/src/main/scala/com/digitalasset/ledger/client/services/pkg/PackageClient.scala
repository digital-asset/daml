// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.pkg

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service._
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.Future

class PackageClient(ledgerId: LedgerId, service: PackageServiceStub) {

  def listPackages(token: Option[String] = None): Future[ListPackagesResponse] =
    LedgerClient.stub(service, token).listPackages(ListPackagesRequest(ledgerId.unwrap))

  def getPackage(packageId: String, token: Option[String] = None): Future[GetPackageResponse] =
    LedgerClient.stub(service, token).getPackage(GetPackageRequest(ledgerId.unwrap, packageId))

  def getPackageStatus(
      packageId: String,
      token: Option[String] = None): Future[GetPackageStatusResponse] =
    LedgerClient
      .stub(service, token)
      .getPackageStatus(GetPackageStatusRequest(ledgerId.unwrap, packageId))
}
