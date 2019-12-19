// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.Scheduler
import akka.pattern.after
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.resources.{Resource, ResourceOwner}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

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
    loggerFactory: NamedLoggerFactory,
) extends AutoCloseable {
  private implicit val executionContext: ExecutionContext = DirectExecutionContext
  private val logger = loggerFactory.getLogger(this.getClass)

  val closed = new AtomicBoolean(false)
  val lastHandle = new AtomicReference[Option[Resource[IndexFeedHandle]]](None)

  /**
    * Starts an indexer, and restarts it after the given delay whenever an error occurs.
    *
    * @param subscribe A function that creates a new indexer and calls subscribe() on it.
    * @return A future that completes with [[akka.Done]] when the indexer finishes processing all read service updates.
    */
  def start(subscribe: () => Resource[IndexFeedHandle]): Resource[Future[Unit]] = {
    val complete = Promise[Unit]()

    logger.info("Starting Indexer Server")
    var subscribeResource = for {
      handle <- subscribe()
    } yield {
      logger.info("Started Indexer Server")
      handle
    }
    resubscribeOnFailure()

    def resubscribe(): Resource[IndexFeedHandle] = {
      logger.info("Starting Indexer Server")
      subscribeResource = for {
        _ <- ResourceOwner
          .forFuture(() => after(restartDelay, scheduler)(Future.successful(())))
          .acquire()
        handle <- subscribe()
      } yield {
        logger.info("Started Indexer Server")
        handle
      }
      resubscribeOnFailure()
      subscribeResource
    }

    def resubscribeOnFailure(): Unit = {
      subscribeResource.asFuture.onComplete {
        case Success(handle) =>
          handle.completed().onComplete {
            case Success(()) =>
              logger.info("Successfully finished processing state updates")
              complete.success(())
              ()

            case Failure(exception) =>
              logger
                .error(
                  s"Error while running indexer, restart scheduled after $restartDelay",
                  exception)
              for {
                _ <- ResourceOwner
                  .forFuture(() => subscribeResource.release().recover { case _ => () })
                  .acquire()
                handle <- resubscribe()
              } yield handle
              ()
          }
        case Failure(exception) =>
          logger
            .error(s"Error while running indexer, restart scheduled after $restartDelay", exception)
          subscribeResource = resubscribe()
      }
    }

    Resource(
      subscribeResource.asFuture
        .transform(_ => Success(complete.future)),
      _ => subscribeResource.release().flatMap(_ => complete.future),
    )
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      val _ = lastHandle.get.foreach(h => Await.result(h.release(), asyncTolerance))
    }
  }
}
