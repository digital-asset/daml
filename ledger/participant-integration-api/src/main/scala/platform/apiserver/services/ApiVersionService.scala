package com.daml.platform.apiserver.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.v1.version_service.GetApiVersionRequest
import com.daml.ledger.api.v1.version_service.GetApiVersionResponse
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc
import com.daml.ledger.api.v1.version_service.VersionServiceGrpc.VersionService
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

private[apiserver] final class ApiVersionService private(implicit loggingContext: LoggingContext)
  extends VersionService with GrpcApiService {

  @volatile var closed = false

  private val logger = ContextualizedLogger.get(this.getClass)

  override def getApiVersion(request: GetApiVersionRequest): Future[GetApiVersionResponse] = {
    Future.successful {
      logger.warn(s"VERSION: 6.6.6")
      GetApiVersionResponse("6.6.6")
    }
  }

  override def bindService(): ServerServiceDefinition =
    VersionServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = closed = true

}

private[apiserver] object ApiVersionService {
  def create()(implicit loggingContext: LoggingContext): ApiVersionService = {
    new ApiVersionService
  }
}