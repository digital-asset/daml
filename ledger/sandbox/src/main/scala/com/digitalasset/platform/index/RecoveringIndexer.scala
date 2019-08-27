// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.Scheduler
import akka.pattern.after
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object RecoveringIndexer {
  def apply(
      scheduler: Scheduler,
      restartDelay: FiniteDuration,
      asyncTolerance: FiniteDuration): RecoveringIndexer =
    new RecoveringIndexer(scheduler, restartDelay, asyncTolerance)
}

/**
  * A helper that restarts an indexer whenever an error occurs.
  *
  * @param restartDelay Time to wait before restarting the indexer after a failure
  * @param asyncTolerance Time to wait for asynchronous operations to complete
  */
class RecoveringIndexer(
    scheduler: Scheduler,
    restartDelay: FiniteDuration,
    asyncTolerance: FiniteDuration)
    extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val closed = new AtomicBoolean(false)
  val lastHandle = new AtomicReference[Option[IndexFeedHandle]](None)

  /**
    * Starts an indexer, and restarts it after the given delay whenever an error occurs.
    *
    * @param subscribe A function that creates a new indexer and calls subscribe() on it.
    * @return A future that completes with [[akka.Done]] when the indexer finishes processing all read service updates.
    */
  def start(subscribe: () => Future[IndexFeedHandle]): Future[akka.Done] = {
    logger.info("Starting Indexer Server")
    implicit val ec: ExecutionContext = DEC
    val completedF = for {
      handle <- subscribe()
      _ = {
        logger.info("Started Indexer Server")
        lastHandle.set(Some(handle))
      }
      completed <- handle.completed()
      _ = logger.info("Successfully finished processing state updates")
    } yield completed

    completedF.recoverWith {
      case NonFatal(t) =>
        logger.error(s"Error while running indexer, restart scheduled after $restartDelay", t)
        lastHandle.set(None)
        after(restartDelay, scheduler)(start(subscribe))
    }(DEC)
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val _ = lastHandle.get.foreach(h => Await.result(h.stop(), asyncTolerance))
    }
  }
}
