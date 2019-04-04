// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.testkit.services

import com.digitalasset.ledger.api.v1.package_service._
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class PackageServiceImpl(
    listPackagesResponse: Future[ListPackagesResponse],
    getPackageResponse: Future[GetPackageResponse],
    getPackageStatusResponse: Future[GetPackageStatusResponse])
    extends PackageService {

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
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] = {
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
      getPackageStatusResponse: Future[GetPackageStatusResponse])(
      implicit ec: ExecutionContext): (ServerServiceDefinition, PackageServiceImpl) = {
    val impl =
      new PackageServiceImpl(listPackagesResponse, getPackageResponse, getPackageStatusResponse)
    (PackageServiceGrpc.bindService(impl, ec), impl)
  }
}
