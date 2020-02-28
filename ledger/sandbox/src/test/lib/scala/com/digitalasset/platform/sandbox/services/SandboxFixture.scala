// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.io.File
import java.net.InetAddress
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.TimeModel
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.auth.client.LedgerCallCredentials
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource, SuiteResource}
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.ports.Port
import com.digitalasset.resources.ResourceOwner
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

trait SandboxFixture extends SuiteResource[(SandboxServer, Channel)] with BeforeAndAfterAll {
  self: Suite =>

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[this] val actorSystemName = this.getClass.getSimpleName

  private lazy val executionContext = ExecutionContext.fromExecutorService(
    Executors.newCachedThreadPool(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(s"$actorSystemName-thread-pool-worker-%d")
        .setUncaughtExceptionHandler((thread, _) =>
          logger.error(s"got an uncaught exception on thread: ${thread.getName}"))
        .build()))

  protected implicit val system: ActorSystem =
    ActorSystem(actorSystemName, defaultExecutionContext = Some(executionContext))

  protected implicit val materializer: Materializer = Materializer(system)

  protected implicit val executionSequencerFactory: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

  override protected def afterAll(): Unit = {
    executionSequencerFactory.close()
    materializer.shutdown()
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }

  protected def darFile = new File(rlocation("ledger/test-common/Test-stable.dar"))

  protected def ledgerId(token: Option[String] = None): domain.LedgerId =
    domain.LedgerId(
      LedgerIdentityServiceGrpc
        .blockingStub(channel)
        .withCallCredentials(token.map(new LedgerCallCredentials(_)).orNull)
        .getLedgerIdentity(GetLedgerIdentityRequest())
        .ledgerId)

  protected def getTimeProviderForClient(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory): TimeProvider = {
    Try(TimeServiceGrpc.stub(channel))
      .map(StaticTime.updatedVia(_, ledgerId().unwrap)(mat, esf))
      .fold[TimeProvider](_ => TimeProvider.UTC, Await.result(_, 30.seconds))
  }

  protected def config: SandboxConfig =
    SandboxConfig.default.copy(
      port = Port.Dynamic,
      damlPackages = packageFiles,
      timeProviderType = Some(TimeProviderType.Static),
      timeModel = TimeModel.reasonableDefault,
      scenario = scenario,
      ledgerIdMode = LedgerIdMode.Static(LedgerId("sandbox-server")),
    )

  protected def packageFiles: List[File] = List(darFile)

  protected def scenario: Option[String] = None

  protected def database: Option[ResourceOwner[String]] = None

  protected def server: SandboxServer = suiteResource.value._1

  protected def serverHost: String = InetAddress.getLoopbackAddress.getHostName

  protected def serverPort: Port = server.port

  protected def channel: Channel = suiteResource.value._2

  override protected lazy val suiteResource: Resource[(SandboxServer, Channel)] = {
    implicit val ec: ExecutionContext = executionContext
    new OwnedResource[(SandboxServer, Channel)](
      for {
        jdbcUrl <- database
          .fold[ResourceOwner[Option[String]]](ResourceOwner.successful(None))(_.map(Some(_)))
        server <- SandboxServer.owner(config.copy(jdbcUrl = jdbcUrl))
        channel <- SandboxClientResource.owner(server.port)
      } yield (server, channel)
    )
  }
}
