// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.version_service.GetApiVersionRequest
import com.daml.ledger.api.v1.version_service.GetApiVersionResponse
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import com.daml.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future
import scala.io.Source

private[apiserver] final class ApiVersionService private
    extends VersionService
    with GrpcApiService {

  @volatile var closed = false
  private val versionFile: String = "ledger-api/VERSION"
  private lazy val apiVersion: String = readVersion(versionFile)

  override def getApiVersion(request: GetApiVersionRequest): Future[GetApiVersionResponse] =
    Future.successful {
      GetApiVersionResponse(apiVersion)
    }

  private def readVersion(versionFileName: String): String = {
    Source
      .fromResource(versionFileName)
      .getLines()
      .toList
      .head
  }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = closed = true

}

private[apiserver] object ApiVersionService {
  def create(): ApiVersionService =
    new ApiVersionService
}
