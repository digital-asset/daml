// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.pkg

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service._
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.client.LedgerClient
import scalaz.syntax.tag._

import scala.concurrent.Future

class PackageClientWithoutLedgerId(service: PackageServiceStub) {

  def listPackages(
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[ListPackagesResponse] =
    LedgerClient
      .stub(service, token)
      .listPackages(ListPackagesRequest(ledgerIdToUse.unwrap))

  def getPackage(
      packageId: String,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetPackageResponse] =
    LedgerClient
      .stub(service, token)
      .getPackage(GetPackageRequest(ledgerIdToUse.unwrap, packageId))

  def getPackageStatus(
      packageId: String,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId,
  ): Future[GetPackageStatusResponse] =
    LedgerClient
      .stub(service, token)
      .getPackageStatus(
        GetPackageStatusRequest(ledgerIdToUse.unwrap, packageId)
      )
}

class PackageClient(val ledgerId: LedgerId, service: PackageServiceStub)
    extends PackageClientWithoutLedgerId(service) {

  override def listPackages(
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[ListPackagesResponse] = super.listPackages(token, ledgerId)
  override def getPackage(
      packageId: String,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetPackageResponse] = super.getPackage(packageId, token, ledgerId)

  override def getPackageStatus(
      packageId: String,
      token: Option[String] = None,
      ledgerIdToUse: LedgerId = ledgerId,
  ): Future[GetPackageStatusResponse] = super.getPackageStatus(packageId, token, ledgerId)
}
