// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.pkg

import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageServiceStub
import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  GetPackageResponse,
  GetPackageStatusRequest,
  GetPackageStatusResponse,
  ListPackagesRequest,
  ListPackagesResponse,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

final class PackageClient(service: PackageServiceStub) {

  def listPackages(
      token: Option[String] = None
  )(implicit traceContext: TraceContext): Future[ListPackagesResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .listPackages(ListPackagesRequest())

  def getPackage(
      packageId: String,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[GetPackageResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .getPackage(GetPackageRequest(packageId = packageId))

  def getPackageStatus(
      packageId: String,
      token: Option[String] = None,
  )(implicit traceContext: TraceContext): Future[GetPackageStatusResponse] =
    LedgerClient
      .stubWithTracing(service, token)
      .getPackageStatus(
        GetPackageStatusRequest(packageId = packageId)
      )
}
