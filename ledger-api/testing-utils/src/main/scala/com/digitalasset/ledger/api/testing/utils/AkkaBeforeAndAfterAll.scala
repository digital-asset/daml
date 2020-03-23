// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

trait AkkaBeforeAndAfterAll extends BeforeAndAfterAll {
  self: Suite =>
  private val logger = LoggerFactory.getLogger(getClass)

  protected def actorSystemName: String = this.getClass.getSimpleName

  protected lazy val akkaExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(
      Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat(s"$actorSystemName-thread-pool-worker-%d")
          .setUncaughtExceptionHandler((thread, _) =>
            logger.error(s"got an uncaught exception on thread: ${thread.getName}"))
          .build()))

  protected implicit lazy val system: ActorSystem =
    ActorSystem(actorSystemName, defaultExecutionContext = Some(akkaExecutionContext))

  protected implicit lazy val materializer: Materializer = Materializer(system)

  protected implicit lazy val executionSequencerFactory: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(poolName = actorSystemName, actorCount = 1)

  override protected def afterAll(): Unit = {
    executionSequencerFactory.close()
    materializer.shutdown()
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }
}
