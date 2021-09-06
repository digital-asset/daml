// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionRequest
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import io.grpc.Status

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal

private[apiserver] final class ApiVersionService private (implicit
    loggingContext: LoggingContext,
    ec: ExecutionContext,
) extends VersionService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val versionFile: String = "ledger-api/VERSION"
  private lazy val apiVersion: Try[String] = readVersion(versionFile)

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest
  ): Future[GetLedgerApiVersionResponse] =
    Future
      .fromTry(apiVersion)
      .map(GetLedgerApiVersionResponse(_))
      .andThen(logger.logErrorsOnCall[GetLedgerApiVersionResponse])
      .recoverWith { case NonFatal(_) =>
        internalError
      }

  private lazy val internalError: Future[Nothing] =
    Future.failed(
      // TODO self-service error codes: Refactor using the new API
      Status.INTERNAL
        .withDescription("Cannot read Ledger API version")
        .asRuntimeException()
    )

  private def readVersion(versionFileName: String): Try[String] =
    Try {
      Source
        .fromResource(versionFileName)
        .getLines()
        .toList
        .head
    }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, ec)

  override def close(): Unit = ()

}

private[apiserver] object ApiVersionService {
  def create()(implicit loggingContext: LoggingContext, ec: ExecutionContext): ApiVersionService =
    new ApiVersionService
}
