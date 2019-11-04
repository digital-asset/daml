// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.testing.utils.{LedgerBackend, MultiResourceBase, Resource}
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.LedgerFactories.SandboxStore
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.AsyncTestSuite
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

trait MultiLedgerFixture extends MultiResourceBase[LedgerBackend, LedgerContext] {
  self: AsyncTestSuite =>

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[this] val actorSystemName = this.getClass.getSimpleName

  private lazy val executorContext = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(s"${actorSystemName}-thread-pool-worker-%d")
        .setUncaughtExceptionHandler((thread, _) =>
          logger.error(s"got an uncaught exception on thread: ${thread.getName}"))
        .build()))

  protected implicit val system: ActorSystem =
    ActorSystem(actorSystemName, defaultExecutionContext = Some(executorContext))

  protected implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  protected implicit val executionSequencerFactory: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

  override protected def afterAll(): Unit = {
    executionSequencerFactory.close()
    materializer.shutdown()
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }

  protected type Config = PlatformApplications.Config

  protected def Config: PlatformApplications.Config.type = PlatformApplications.Config

  protected def config: Config

  protected def basePort = 6865

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  override protected def fixtureIdsEnabled: Set[LedgerBackend] =
    Set(LedgerBackend.SandboxInMemory, LedgerBackend.SandboxSql)

  override protected def constructResource(
      index: Int,
      fixtureId: LedgerBackend): Resource[LedgerContext] = {
    fixtureId match {
      case LedgerBackend.SandboxInMemory =>
        LedgerFactories.createSandboxResource(config, SandboxStore.InMemory)
      case LedgerBackend.SandboxSql =>
        LedgerFactories.createSandboxResource(config, SandboxStore.Postgres)
    }
  }
}
