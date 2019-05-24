// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.pkg

import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService

import scalaz.syntax.tag._

import scala.concurrent.Future

class PackageClient(ledgerId: LedgerId, packageService: PackageService) {

  def listPackages(): Future[ListPackagesResponse] =
    packageService.listPackages(ListPackagesRequest(ledgerId.unwrap))

  def getPackage(packageId: String): Future[GetPackageResponse] =
    packageService.getPackage(GetPackageRequest(ledgerId.unwrap, packageId))

  def getPackageStatus(packageId: String): Future[GetPackageStatusResponse] =
    packageService
      .getPackageStatus(GetPackageStatusRequest(ledgerId.unwrap, packageId))
}
