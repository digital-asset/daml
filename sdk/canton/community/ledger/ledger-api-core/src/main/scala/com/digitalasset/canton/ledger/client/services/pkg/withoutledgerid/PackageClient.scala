// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.pkg.withoutledgerid

import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.api.v1.package_service.*
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.client.LedgerClient
import scalaz.syntax.tag.*

import scala.concurrent.Future

final class PackageClient(service: PackageServiceStub) {

  def listPackages(
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Future[ListPackagesResponse] =
    LedgerClient
      .stub(service, token)
      .listPackages(ListPackagesRequest(ledgerIdToUse.unwrap))

  def getPackage(
      packageId: String,
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Future[GetPackageResponse] =
    LedgerClient
      .stub(service, token)
      .getPackage(GetPackageRequest(ledgerIdToUse.unwrap, packageId))

  def getPackageStatus(
      packageId: String,
      ledgerIdToUse: LedgerId,
      token: Option[String] = None,
  ): Future[GetPackageStatusResponse] =
    LedgerClient
      .stub(service, token)
      .getPackageStatus(
        GetPackageStatusRequest(ledgerIdToUse.unwrap, packageId)
      )
}
