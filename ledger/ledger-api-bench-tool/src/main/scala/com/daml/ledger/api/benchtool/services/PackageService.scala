// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.package_service._
import io.grpc.Channel

import scala.concurrent.Future

class PackageService(channel: Channel, authorizationToken: Option[String]) {
  private val service =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(PackageServiceGrpc.stub(channel))

  def getPackage(packageId: String): Future[GetPackageResponse] = {
    service.getPackage(GetPackageRequest(packageId = packageId))
  }

  def listPackages(): Future[ListPackagesResponse] = {
    service.listPackages(ListPackagesRequest())
  }
}
