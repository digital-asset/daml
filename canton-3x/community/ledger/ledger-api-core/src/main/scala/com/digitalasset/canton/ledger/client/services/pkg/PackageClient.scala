// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.pkg

import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.api.v1.package_service.*
import com.digitalasset.canton.ledger.client.LedgerClient

import scala.concurrent.Future

final class PackageClient(service: PackageServiceStub) {

  def listPackages(
      token: Option[String] = None
  ): Future[ListPackagesResponse] =
    LedgerClient
      .stub(service, token)
      .listPackages(ListPackagesRequest())

  def getPackage(
      packageId: String,
      token: Option[String] = None,
  ): Future[GetPackageResponse] =
    LedgerClient
      .stub(service, token)
      .getPackage(GetPackageRequest(packageId = packageId))

  def getPackageStatus(
      packageId: String,
      token: Option[String] = None,
  ): Future[GetPackageStatusResponse] =
    LedgerClient
      .stub(service, token)
      .getPackageStatus(
        GetPackageStatusRequest(packageId = packageId)
      )
}
