// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import akka.pattern.after
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.resources.Resource

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
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
) {
  private implicit val executionContext: ExecutionContext = DirectExecutionContext
  private val logger = loggerFactory.getLogger(this.getClass)

  /**
    * Starts an indexer, and restarts it after the given delay whenever an error occurs.
    *
    * @param subscribe A function that creates a new indexer and calls subscribe() on it.
    * @return A future that completes with [[akka.Done]] when the indexer finishes processing all read service updates.
    */
  def start(subscribe: () => Resource[IndexFeedHandle]): Resource[Future[Unit]] = {
    val complete = Promise[Unit]()

    logger.info("Starting Indexer Server")
    val subscription = new AtomicReference[Resource[IndexFeedHandle]](null)

    val firstSubscription = subscribe().map(handle => {
      logger.info("Started Indexer Server")
      handle
    })
    subscription.set(firstSubscription)
    resubscribeOnFailure(firstSubscription)

    def resubscribe(oldSubscription: Resource[IndexFeedHandle]): Future[Unit] =
      for {
        _ <- after(restartDelay, scheduler)(Future.successful(()))
        _ <- {
          logger.info("Restarting Indexer Server")
          val newSubscription = subscribe()
          if (subscription.compareAndSet(oldSubscription, newSubscription)) {
            resubscribeOnFailure(newSubscription)
            newSubscription.asFuture.map { _ =>
              logger.info("Restarted Indexer Server")
            }
          } else { // we must have stopped the server during the restart
            logger.info("Indexer Server was stopped; cancelling the restart")
            newSubscription.release().flatMap { _ =>
              logger.info("Indexer Server restart was cancelled")
              complete.trySuccess(())
              complete.future
            }
          }
        }
      } yield ()

    def resubscribeOnFailure(currentSubscription: Resource[IndexFeedHandle]): Unit =
      currentSubscription.asFuture.onComplete {
        case Success(handle) =>
          handle.completed().onComplete {
            case Success(()) =>
              logger.info("Successfully finished processing state updates")
              complete.trySuccess(())
              complete.future

            case Failure(exception) =>
              logger.error(
                s"Error while running indexer, restart scheduled after $restartDelay",
                exception)
              currentSubscription
                .release()
                .recover { case _ => () } // releasing may yield the same error as above
                .flatMap(_ => resubscribe(currentSubscription))
          }
        case Failure(exception) =>
          logger
            .error(
              s"Error while starting indexer, restart scheduled after $restartDelay",
              exception)
          resubscribe(currentSubscription)
          ()
      }

    Resource(
      subscription
        .get()
        .asFuture
        .transform(_ => Success(complete.future)),
      _ => {
        logger.info("Stopping Indexer Server")
        subscription
          .getAndSet(null)
          .release()
          .flatMap(_ => complete.future)
          .map(_ => {
            logger.info("Stopped Indexer Server")
          })
      },
    )
  }
}
