// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.testing.utils

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait AkkaBeforeAndAfterAll extends BeforeAndAfterAll {
  self: Suite =>
  protected def actorSystemName = this.getClass.getSimpleName

  private val executorContext = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(s"${actorSystemName}-thread-pool-worker-%d")
        .setUncaughtExceptionHandler((thread, _) =>
          println(s"got an uncaught exception on thread: ${thread.getName}"))
        .build()))

  protected implicit val system: ActorSystem =
    ActorSystem(actorSystemName, defaultExecutionContext = Some(executorContext))

  protected implicit val materializer: ActorMaterializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    materializer.shutdown()
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }
}
