// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.lf.data.Ref
import com.daml.daml_lf_dev.DamlLf.{Archive, HashFunction}
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service.HashFunction.{
  SHA256 => APISHA256,
  Unrecognized => APIUnrecognized
}
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service.{HashFunction => APIHashFunction, _}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.PackageServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition, Status}

import scala.concurrent.Future

final class ApiPackageService private (backend: IndexPackagesService)(
    implicit logCtx: LoggingContext)
    extends PackageService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, DEC)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
    backend
      .listLfPackages()
      .map(p => ListPackagesResponse(p.keys.toSeq))(DEC)
      .andThen(logger.logErrorsOnCall[ListPackagesResponse])(DEC)

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .getLfArchive(pId)
          .flatMap(_.fold(Future.failed[GetPackageResponse](Status.NOT_FOUND.asRuntimeException()))(
            archive => Future.successful(toGetPackageResponse(archive))))(DEC)
          .andThen(logger.logErrorsOnCall[GetPackageResponse])(DEC)
    )

  override def getPackageStatus(
      request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
    withValidatedPackageId(
      request.packageId,
      pId =>
        backend
          .listLfPackages()
          .map { packages =>
            val result = if (packages.contains(pId)) {
              PackageStatus.REGISTERED
            } else {
              PackageStatus.UNKNOWN
            }
            GetPackageStatusResponse(result)
          }(DEC)
          .andThen(logger.logErrorsOnCall[GetPackageStatusResponse])(DEC)
    )

  private def withValidatedPackageId[T](packageId: String, block: Ref.PackageId => Future[T]) =
    Ref.PackageId
      .fromString(packageId)
      .fold(
        error =>
          Future.failed[T](
            Status.INVALID_ARGUMENT
              .withDescription(error)
              .asRuntimeException()),
        pId => block(pId)
      )

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashF: APIHashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APISHA256
      case _ => APIUnrecognized(-1)
    }
    GetPackageResponse(hashF, archive.getPayload, archive.getHash)
  }

}

object ApiPackageService {
  def create(ledgerId: LedgerId, backend: IndexPackagesService)(
      implicit logCtx: LoggingContext): PackageService with GrpcApiService =
    new PackageServiceValidation(new ApiPackageService(backend), ledgerId) with BindableService {
      override def bindService(): ServerServiceDefinition =
        PackageServiceGrpc.bindService(this, DEC)
    }
}
