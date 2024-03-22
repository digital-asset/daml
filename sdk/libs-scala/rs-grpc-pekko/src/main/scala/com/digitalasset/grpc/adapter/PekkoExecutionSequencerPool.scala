// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter

import java.util.concurrent.atomic.AtomicInteger

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

class PekkoExecutionSequencerPool(
    poolName: String,
    actorCount: Int = PekkoExecutionSequencerPool.defaultActorCount,
    terminationTimeout: FiniteDuration = 30.seconds,
)(implicit system: ActorSystem)
    extends ExecutionSequencerFactory {
  require(actorCount > 0)

  private val counter = new AtomicInteger()

  private val pool =
    Array.fill(actorCount)(
      PekkoExecutionSequencer(s"$poolName-${counter.getAndIncrement()}", terminationTimeout)
    )

  override def getExecutionSequencer: ExecutionSequencer =
    pool(counter.getAndIncrement() % actorCount)

  override def close(): Unit =
    Await.result(closeAsync(), terminationTimeout)

  def closeAsync(): Future[Unit] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val eventuallyClosed: Future[Seq[Done]] = Future.sequence(pool.view.map(_.closeAsync).toSeq)
    Future.firstCompletedOf(
      Seq(
        system.whenTerminated.map(_ => ()), //  Cut it short if the ActorSystem stops.
        eventuallyClosed.map(_ => ()),
      )
    )
  }
}

object PekkoExecutionSequencerPool {

  /** Spread 8 actors per virtual core in order to mitigate head of line blocking.
    * The number 8 was chosen somewhat arbitrarily, but seems to help performance.
    */
  private val defaultActorCount: Int = Runtime.getRuntime.availableProcessors() * 8
}
