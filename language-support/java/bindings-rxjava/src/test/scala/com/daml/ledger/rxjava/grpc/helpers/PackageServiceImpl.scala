// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import com.daml.ledger.api.v1.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  ListPackagesResponse,
}
import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  GetPackageStatusRequest,
  ListPackagesRequest,
  PackageServiceGrpc,
}
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.PackageServiceV2Authorization
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class PackageServiceImpl(
    listPackagesResponse: Future[ListPackagesResponse],
    getPackageResponse: Future[GetPackageResponse],
    getPackageStatusResponse: Future[GetPackageStatusResponse],
) extends PackageService
    with FakeAutoCloseable {

  private var lastListPackageRequest: Option[ListPackagesRequest] = None
  private var lastGetPackagesRequest: Option[GetPackageRequest] = None
  private var lastGetPackageStatusRequest: Option[GetPackageStatusRequest] = None

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    this.lastListPackageRequest = Some(request)
    listPackagesResponse
  }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] = {
    this.lastGetPackagesRequest = Some(request)
    getPackageResponse
  }

  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] = {
    this.lastGetPackageStatusRequest = Some(request)
    getPackageStatusResponse
  }

  def getLastListPackageRequest: Option[ListPackagesRequest] = this.lastListPackageRequest
  def getLastGetPackagesRequest: Option[GetPackageRequest] = this.lastGetPackagesRequest
  def getLastGetPackageStatusRequest: Option[GetPackageStatusRequest] =
    this.lastGetPackageStatusRequest
}

object PackageServiceImpl {

  def createWithRef(
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse],
      authorizer: Authorizer,
  )(implicit ec: ExecutionContext): (ServerServiceDefinition, PackageServiceImpl) = {
    val impl =
      new PackageServiceImpl(listPackagesResponse, getPackageResponse, getPackageStatusResponse)
    val authImpl = new PackageServiceV2Authorization(impl, authorizer)
    (PackageServiceGrpc.bindService(authImpl, ec), impl)
  }
}
