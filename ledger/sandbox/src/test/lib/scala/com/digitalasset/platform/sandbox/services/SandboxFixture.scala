// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.io.File
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
import com.digitalasset.ledger.api.testing.utils.{Resource, SuiteResource}
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.grpc.Channel
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

trait SandboxFixture extends SuiteResource[Unit] with BeforeAndAfterAll {
  self: Suite =>

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[this] val actorSystemName = this.getClass.getSimpleName

  private lazy val executorContext = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(s"$actorSystemName-thread-pool-worker-%d")
        .setUncaughtExceptionHandler((thread, _) =>
          logger.error(s"got an uncaught exception on thread: ${thread.getName}"))
        .build()))

  protected implicit val system: ActorSystem =
    ActorSystem(actorSystemName, defaultExecutionContext = Some(executorContext))

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
    SandboxConfig.default
      .copy(
        port = 0, //dynamic port allocation
        damlPackages = packageFiles,
        timeProviderType = TimeProviderType.Static,
        timeModel = TimeModel.reasonableDefault,
        scenario = scenario,
        ledgerIdMode = LedgerIdMode.Static(LedgerId("sandbox-server"))
      )

  protected def packageFiles: List[File] = List(darFile)

  protected def scenario: Option[String] = None

  protected def getSandboxPort: Int = serverResource.value

  protected def channel: Channel = clientResource.value

  protected lazy val serverResource = new SandboxServerResource(config)

  protected lazy val clientResource = new SandboxClientResource(() => serverResource.value)

  protected override lazy val suiteResource: Resource[Unit] = new Resource[Unit] {
    override val value: Unit = ()

    override def setup(): Unit = {
      serverResource.setup()
      clientResource.setup()
    }

    override def close(): Unit = {
      clientResource.close()
      serverResource.close()
    }
  }
}
