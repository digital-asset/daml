// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.Scheduler
import akka.pattern.after
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.resources.Resource

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * A helper that restarts an indexer whenever an error occurs.
  *
  * @param restartDelay Time to wait before restarting the indexer after a failure
  * @param asyncTolerance Time to wait for asynchronous operations to complete
  */
class RecoveringIndexer(
    scheduler: Scheduler,
    restartDelay: FiniteDuration,
    asyncTolerance: FiniteDuration,
    loggerFactory: NamedLoggerFactory)
    extends AutoCloseable {
  private val logger = loggerFactory.getLogger(this.getClass)

  val closed = new AtomicBoolean(false)
  val lastHandle = new AtomicReference[Option[Resource[IndexFeedHandle]]](None)

  /**
    * Starts an indexer, and restarts it after the given delay whenever an error occurs.
    *
    * @param subscribe A function that creates a new indexer and calls subscribe() on it.
    * @return A future that completes with [[akka.Done]] when the indexer finishes processing all read service updates.
    */
  def start(subscribe: () => Resource[IndexFeedHandle]): Future[akka.Done] = {
    logger.info("Starting Indexer Server")
    implicit val ec: ExecutionContext = DEC

    val subscribeResource = for {
      handle <- subscribe()
    } yield {
      logger.info("Started Indexer Server")
      handle
    }

    val completedF = for {
      handle <- subscribeResource.asFuture
      _ = lastHandle.set(Some(subscribeResource))
      completed <- handle.completed()
      _ = logger.info("Successfully finished processing state updates")
    } yield completed

    completedF.recoverWith {
      case NonFatal(t) =>
        logger.error(s"Error while running indexer, restart scheduled after $restartDelay", t)
        lastHandle.set(None)
        subscribeResource
          .release()
          .recover {
            case _ => ()
          }
          .flatMap(_ => after(restartDelay, scheduler)(start(subscribe)))
    }(DEC)
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val _ = lastHandle.get.foreach(h => Await.result(h.release(), asyncTolerance))
    }
  }
}
