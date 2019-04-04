// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter

import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.actor.ActorSystem

import scala.collection.breakOut
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class AkkaExecutionSequencerPool(
    poolName: String,
    actorCount: Int = AkkaExecutionSequencerPool.defaultActorCount,
    terminationTimeout: FiniteDuration = 30.seconds)(implicit system: ActorSystem)
    extends ExecutionSequencerFactory {
  require(actorCount > 0)

  private val counter = new AtomicInteger()

  private val pool =
    Array.fill(actorCount)(
      AkkaExecutionSequencer(s"$poolName-${counter.getAndIncrement()}", terminationTimeout))

  override def getExecutionSequencer: ExecutionSequencer =
    pool(counter.getAndIncrement() % actorCount)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def close(): Unit = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    val eventualDones: Iterable[Future[Done]] = pool.map(_.closeAsync)(breakOut)
    Await.result(
      Future.firstCompletedOf[Any](
        List(
          system.whenTerminated, //  Cut it short if the actorsystem is down already
          Future.sequence(eventualDones))),
      terminationTimeout
    )
    ()
  }
}

object AkkaExecutionSequencerPool {

  /** Spread 8 actors per virtual core in order to mitigate head of line blocking.
    * The number 8 was chosen somewhat arbitrarily, but seems to help performance. */
  private val defaultActorCount: Int = Runtime.getRuntime.availableProcessors() * 8
}
