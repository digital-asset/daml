// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionRequest
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future
import scala.io.Source
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[apiserver] final class ApiVersionService private (implicit loggingContext: LoggingContext)
    extends VersionService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val versionFile: String = "ledger-api/VERSION"
  private lazy val apiVersion: String = readVersion(versionFile)

  override def getLedgerApiVersion(
      request: GetLedgerApiVersionRequest): Future[GetLedgerApiVersionResponse] =
    Future.successful {
      GetLedgerApiVersionResponse(apiVersion)
    }

  private def readVersion(versionFileName: String): String = {
    Try {
      Source
        .fromResource(versionFileName)
        .getLines()
        .toList
        .head
    } match {
      case Success(version) => version
      case Failure(ex) =>
        logger.error(s"An error occurred when reading the Ledger API version", ex)
        throw ex
    }

  }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = ()

}

private[apiserver] object ApiVersionService {
  def create()(implicit loggingContext: LoggingContext): ApiVersionService =
    new ApiVersionService
}
