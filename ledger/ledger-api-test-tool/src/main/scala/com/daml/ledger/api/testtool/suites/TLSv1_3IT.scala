// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation.{NoParties, allocate}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import com.daml.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityServiceBlockingStub
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc,
}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.grpc.{Channel, StatusRuntimeException}

import java.util.concurrent._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

final class TLSv1_3IT extends LedgerTestSuite {

  testTlsConnection(clientTls = TlsVersion.V1_3, assertion = assertSuccessfulConnection)
  testTlsConnection(clientTls = TlsVersion.V1_2, assertion = assertFailedlConnection)
  testTlsConnection(clientTls = TlsVersion.V1_1, assertion = assertFailedlConnection)
  testTlsConnection(clientTls = TlsVersion.V1, assertion = assertFailedlConnection)

  def testTlsConnection(clientTls: TlsVersion, assertion: Try[String] => Try[Unit]): Unit = {
    test2(
      "ConnectionOnTLSv1.3",
      "A ledger API server accepts TLS 1.3 connection",
      allocate(NoParties),
    ) { implicit ec => (testContexts: Seq[ParticipantTestContext]) =>
      { case _ =>
        assume(testContexts.nonEmpty, "At least one context expected!")
        val firstTextContext = testContexts.head
        assume(
          firstTextContext.clientTlsConfiguration.isDefined,
          "This test case requires TLS configuration!",
        )
        val tlsConfiguration = firstTextContext.clientTlsConfiguration.get

        val concurrency = Concurrency(
          corePoolSize = 2,
          maxPoolSize = 2,
          keepAliveTime = 1000,
          maxQueueLength = 1000,
        )

        val ledger = Ledger(
          hostname = firstTextContext.ledgerHostname,
          port = firstTextContext.ledgerPort,
        )

        val resources: ResourceOwner[LedgerIdentityServiceBlockingStub] = for {
          executorService <- threadPoolExecutorOwner(concurrency)
          channel <- channelOwner(
            ledger,
            tlsConfiguration,
            executorService,
            clientTlsVersion = clientTls,
          )
          identityService = LedgerIdentityServiceGrpc.blockingStub(channel)
        } yield identityService

        // when
        val actual: Future[String] = resources.use {
          (identityService: LedgerIdentityServiceBlockingStub) =>
            val response = identityService.getLedgerIdentity(new GetLedgerIdentityRequest())
            Future.successful(response.ledgerId)
        }(ResourceContext(ec))

        // then
        actual.transform[Unit] {
          assertion
        }
      }
    }
  }

  private lazy val assertSuccessfulConnection: Try[String] => Try[Unit] = {
    case Success(ledgerId) =>
      Try[Unit] {
        assert(
          assertion = ledgerId ne null,
          message = s"Expected not null ledger id!",
        )
      }
    case Failure(exception) =>
      throw new AssertionError(s"Failed to get a successful server response!", exception)
  }

  private lazy val assertFailedlConnection: Try[String] => Try[Unit] = {
    case Success(ledgerId) =>
      Try[Unit] {
        assert(
          assertion = false,
          message =
            s"Connection succeeded (and returned ledgerId: ${ledgerId} why it should have failed!",
        )
      }
    case Failure(_: StatusRuntimeException) => Success[Unit](())
    case Failure(other) =>
      Try[Unit] {
        assert(
          assertion = false,
          message = s"Unexexpected failure: ${other}",
        )
      }
  }

  // TODO BATKO: below code copied from com.daml.ledger.api.benchtool.LedgerApiBenchTool. There's likely room to simplify

  case class Concurrency(
      corePoolSize: Int,
      maxPoolSize: Int,
      keepAliveTime: Long,
      maxQueueLength: Int,
  )

  case class Ledger(
      hostname: String,
      port: Int,
  )

  def threadPoolExecutorOwner(
      config: Concurrency
  ): ResourceOwner[ThreadPoolExecutor] =
    ResourceOwner.forExecutorService(() =>
      // TODO PBATKO: What are good defaults? Do we even need a thread pool?
      new ThreadPoolExecutor(
        config.corePoolSize,
        config.maxPoolSize,
        config.keepAliveTime,
        TimeUnit.SECONDS,
        if (config.maxQueueLength == 0) new SynchronousQueue[Runnable]()
        else new ArrayBlockingQueue[Runnable](config.maxQueueLength),
      )
    )

  def channelOwner(
      ledger: Ledger,
      tls: TlsConfiguration,
      executor: Executor,
      clientTlsVersion: TlsVersion,
  ): ResourceOwner[Channel] = {
    val MessageChannelSizeBytes: Int = 32 * 1024 * 1024 // 32 MiB
    val ShutdownTimeout: FiniteDuration = 5.seconds

    val channelBuilder = NettyChannelBuilder
      .forAddress(ledger.hostname, ledger.port)
      .executor(executor)
      .maxInboundMessageSize(MessageChannelSizeBytes)
      .usePlaintext()

    if (tls.enabled) {
      tls
        .client(enabledProtocols = Seq(clientTlsVersion))
        .map { sslContext =>
          channelBuilder
            .useTransportSecurity()
            .sslContext(sslContext)
            .negotiationType(NegotiationType.TLS)
        }
    }

    ResourceOwner.forChannel(channelBuilder, ShutdownTimeout)
  }

}
