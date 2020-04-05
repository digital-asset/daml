// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import akka.pattern.after
import com.daml.dec.DirectExecutionContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.resources.Resource

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * A helper that restarts an indexer whenever an error occurs.
  *
  * @param scheduler    Used to schedule the restart operation.
  * @param restartDelay Time to wait before restarting the indexer after a failure
  */
final class RecoveringIndexer(
    scheduler: Scheduler,
    restartDelay: FiniteDuration,
)(implicit logCtx: LoggingContext) {
  private implicit val executionContext: ExecutionContext = DirectExecutionContext
  private val logger = ContextualizedLogger.get(this.getClass)
  private val clock = Clock.systemUTC()

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

    def waitForRestart(
        delayUntil: Instant = clock.instant().plusMillis(restartDelay.toMillis)
    ): Future[Boolean] = {
      val now = clock.instant()
      val delayIncrement =
        Duration.fromNanos(
          math.max(0, math.min(1.second.toNanos, ChronoUnit.NANOS.between(now, delayUntil))))
      after(delayIncrement, scheduler) {
        if (subscription.get() == null) {
          logger.info("Indexer Server was stopped; cancelling the restart")
          complete.trySuccess(())
          complete.future.map(_ => false)
        }
        if (clock.instant().isAfter(delayUntil)) {
          Future.successful(true)
        } else {
          waitForRestart(delayUntil)
        }
      }
    }

    def resubscribe(oldSubscription: Resource[IndexFeedHandle]): Future[Unit] =
      for {
        running <- waitForRestart()
        _ <- {
          if (running) {
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
          } else {
            Future.successful(())
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
        .transform(_ => Success(complete.future)))(
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
