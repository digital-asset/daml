// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.package_service.{GetPackageResponse, ListPackagesResponse, *}
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import io.grpc.Channel

import scala.concurrent.Future

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class PackageService(channel: Channel, authorizationToken: Option[String]) {
  private val service =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(PackageServiceGrpc.stub(channel))

  def getPackage(packageId: String): Future[GetPackageResponse] =
    service.getPackage(GetPackageRequest(packageId = packageId))

  def listPackages(): Future[ListPackagesResponse] =
    service.listPackages(ListPackagesRequest())
}
