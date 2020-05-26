// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem

import scala.collection.breakOut
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}

class AkkaExecutionSequencerPool(
    poolName: String,
    actorCount: Int = AkkaExecutionSequencerPool.defaultActorCount,
    terminationTimeout: FiniteDuration = 30.seconds,
)(implicit system: ActorSystem)
    extends ExecutionSequencerFactory {
  require(actorCount > 0)

  private val counter = new AtomicInteger()

  private val pool =
    Array.fill(actorCount)(
      AkkaExecutionSequencer(s"$poolName-${counter.getAndIncrement()}", terminationTimeout))

  override def getExecutionSequencer: ExecutionSequencer =
    pool(counter.getAndIncrement() % actorCount)

  override def close(): Unit =
    Await.result(closeAsync(), terminationTimeout)

  def closeAsync(): Future[Unit] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val eventuallyClosed: Future[Seq[Done]] = Future.sequence(pool.map(_.closeAsync)(breakOut))
    Future.firstCompletedOf(
      Seq(
        system.whenTerminated.map(_ => ()), //  Cut it short if the ActorSystem stops.
        eventuallyClosed.map(_ => ()),
      )
    )
  }
}

object AkkaExecutionSequencerPool {

  /** Spread 8 actors per virtual core in order to mitigate head of line blocking.
    * The number 8 was chosen somewhat arbitrarily, but seems to help performance. */
  private val defaultActorCount: Int = Runtime.getRuntime.availableProcessors() * 8
}
