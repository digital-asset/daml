// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.logging.FailingServerFixture.Exceptions.{
  AbortedGrpc,
  InternalGrpc
}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CommandCompletionServiceLogging,
  CompletionEndRequest,
  CompletionEndResponse,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
  LedgerIdentityServiceLogging
}
import com.digitalasset.ledger.api.v1.package_service.{
  GetPackageRequest,
  GetPackageResponse,
  GetPackageStatusRequest,
  GetPackageStatusResponse,
  ListPackagesRequest,
  ListPackagesResponse,
  PackageServiceGrpc,
  PackageServiceLogging
}
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc,
  TransactionServiceLogging
}
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import io.grpc.{
  BindableService,
  Channel,
  ManagedChannel,
  Server,
  ServerServiceDefinition,
  Status,
  StatusRuntimeException
}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.global
import scala.concurrent.Future
import scala.util.control.NoStackTrace

trait FailingServerFixture extends BeforeAndAfterAll { self: Suite =>
  import FailingServerFixture._
  private def serverName = "FailingServer"

  private var server: Server = _
  private var channel: ManagedChannel = _

  protected def getChannel: Channel = channel

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server = InProcessServerBuilder
      .forName(serverName)
      .addService(new Identity() with LedgerIdentityServiceLogging)
      .addService(new Completion() with CommandCompletionServiceLogging)
      .addService(new Transaction() with TransactionServiceLogging)
      .addService(new Package() with PackageServiceLogging)
      .build()
    server.start()
    channel = InProcessChannelBuilder.forName(serverName).build()
  }

  override protected def afterAll(): Unit = {
    channel.shutdownNow()
    server.shutdownNow()
    channel.awaitTermination(5L, TimeUnit.SECONDS)
    server.awaitTermination()
  }

}

object FailingServerFixture {

  import Exceptions._
  class Identity extends LedgerIdentityService with BindableService {
    protected val logger = LoggerFactory.getLogger(this.getClass)
    override def bindService(): ServerServiceDefinition =
      LedgerIdentityServiceGrpc.bindService(this, global)
    override def getLedgerIdentity(
        request: GetLedgerIdentityRequest): Future[GetLedgerIdentityResponse] =
      throw UnknownException
  }

  class Completion extends CommandCompletionService with BindableService {
    protected val logger = LoggerFactory.getLogger(this.getClass)
    override def completionStream(
        request: CompletionStreamRequest,
        responseObserver: StreamObserver[CompletionStreamResponse]): Unit =
      responseObserver.onError(UnknownException)
    override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
      Future.failed(UnknownException)
    override def bindService(): ServerServiceDefinition =
      CommandCompletionServiceGrpc.bindService(this, global)
  }

  class Transaction extends TransactionService with BindableService {
    protected val logger = LoggerFactory.getLogger(this.getClass)
    override def getTransactions(
        request: GetTransactionsRequest,
        responseObserver: StreamObserver[GetTransactionsResponse]): Unit =
      responseObserver.onError(AbortedGrpc)
    override def getTransactionTrees(
        request: GetTransactionsRequest,
        responseObserver: StreamObserver[GetTransactionTreesResponse]): Unit =
      responseObserver.onError(InternalGrpc)
    override def getTransactionByEventId(
        request: GetTransactionByEventIdRequest): Future[GetTransactionResponse] =
      throw InternalGrpc
    override def getTransactionById(
        request: GetTransactionByIdRequest): Future[GetTransactionResponse] =
      Future.failed(AbortedGrpc)
    override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
      throw AbortedGrpc
    override def bindService(): ServerServiceDefinition =
      TransactionServiceGrpc.bindService(this, global)
  }

  class Package extends PackageService with BindableService {
    protected val logger = LoggerFactory.getLogger(this.getClass)
    override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] =
      Future.failed(InternalGrpc)
    override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
      Future.failed(InternalGrpc)
    override def getPackageStatus(
        request: GetPackageStatusRequest): Future[GetPackageStatusResponse] =
      Future.failed(InternalGrpc)
    override def bindService(): ServerServiceDefinition =
      PackageServiceGrpc.bindService(this, global)
  }

  object Exceptions {
    object UnknownException
        extends RuntimeException("Who knows where this came from. Must be logged.")
        with NoStackTrace

    object AbortedGrpc
        extends StatusRuntimeException(
          Status.ABORTED.withDescription("This is fine. Must not be logged."))
        with NoStackTrace

    object InternalGrpc
        extends StatusRuntimeException(
          Status.INTERNAL.withDescription("This is scary. Must be logged."))
        with NoStackTrace
  }
}
