// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.time.temporal.ChronoUnit
import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import akka.pattern.after
import com.daml.ledger.api.health.{HealthStatus, Healthy, ReportsHealth, Unhealthy}
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.duration.{Duration, DurationLong, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** A helper that restarts an indexer whenever an error occurs.
  *
  * @param scheduler    Used to schedule the restart operation.
  * @param restartDelay Time to wait before restarting the indexer after a failure
  */
private[indexer] final class RecoveringIndexer(
    scheduler: Scheduler,
    executionContext: ExecutionContext,
    restartDelay: FiniteDuration,
    updateHealthStatus: HealthStatus => Unit,
    healthReporter: ReportsHealth,
)(implicit loggingContext: LoggingContext) {
  private implicit val ec: ExecutionContext = executionContext
  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private val logger = ContextualizedLogger.get(this.getClass)
  private val clock = Clock.systemUTC()

  /** Starts an indexer, and restarts it after the given delay whenever an error occurs.
    *
    * @param indexer A ResourceOwner for indexing
    * @return A future that completes with [[akka.Done]] when the indexer finishes processing all read service updates.
    */
  def start(indexer: Indexer): Resource[(ReportsHealth, Future[Unit])] = {
    val complete = Promise[Unit]()

    logger.info("Starting Indexer Server")
    val subscription = new AtomicReference[Resource[Future[Unit]]](null)

    val firstSubscription = indexer
      .acquire()
      .map(handle => {
        logger.info("Started Indexer Server")
        updateHealthStatus(Healthy)
        handle
      })
    subscription.set(firstSubscription)
    resubscribeOnFailure(firstSubscription) {}

    def waitForRestart(
        delayUntil: Instant = clock.instant().plusMillis(restartDelay.toMillis)
    ): Future[Boolean] = {
      val now = clock.instant()
      val delay = Duration.fromNanos(ChronoUnit.NANOS.between(now, delayUntil))
      // If `after` is passed a duration of zero, it executes the block synchronously,
      // which can lead to stack overflows.
      val delayIncrement = delay.min(1.second).max(1.nanosecond)
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

    def resubscribe(oldSubscription: Resource[Future[Unit]]): Future[Unit] =
      for {
        running <- waitForRestart()
        _ <- {
          if (running) {
            logger.info("Restarting Indexer Server")
            val newSubscription = indexer.acquire()
            if (subscription.compareAndSet(oldSubscription, newSubscription)) {
              resubscribeOnFailure(newSubscription) {
                updateHealthStatus(HealthStatus.healthy)
                logger.info("Restarted Indexer Server")
              }
              Future.unit
            } else { // we must have stopped the server during the restart
              logger.info("Indexer Server was stopped; cancelling the restart")
              newSubscription.release().flatMap { _ =>
                logger.info("Indexer Server restart was cancelled")
                complete.trySuccess(())
                complete.future
              }
            }
          } else {
            Future.unit
          }
        }
      } yield ()

    def resubscribeOnFailure(
        currentSubscription: Resource[Future[Unit]]
    )(actOnSuccess: => Unit): Unit =
      currentSubscription.asFuture.onComplete {
        case Success(handle) =>
          actOnSuccess
          handle.onComplete {
            case Success(()) =>
              logger.info("Successfully finished processing state updates")
              complete.trySuccess(())
              complete.future

            case Failure(exception) =>
              reportErrorState(
                s"Error while running indexer, restart scheduled after $restartDelay",
                exception,
              )

              currentSubscription
                .release()
                .recover { case _ => () } // releasing may yield the same error as above
                .flatMap(_ => resubscribe(currentSubscription))
          }
        case Failure(exception) =>
          reportErrorState(
            s"Error while starting indexer, restart scheduled after $restartDelay",
            exception,
          )
          resubscribe(currentSubscription)
          ()
      }

    Resource(
      subscription
        .get()
        .asFuture
        .transform(_ => Success(healthReporter -> complete.future))
    )(_ => {
      logger.info("Stopping Indexer Server")
      subscription
        .getAndSet(null)
        .release()
        .flatMap(_ => complete.future)
        .map(_ => {
          updateHealthStatus(Unhealthy)
          logger.info("Stopped Indexer Server")
        })
    })
  }

  private def reportErrorState(errorMessage: String, exception: Throwable): Unit = {
    updateHealthStatus(Unhealthy)
    logger.error(errorMessage, exception)
  }
}

private[indexer] object RecoveringIndexer {
  def apply(scheduler: Scheduler, executionContext: ExecutionContext, restartDelay: FiniteDuration)(
      implicit loggingContext: LoggingContext
  ): RecoveringIndexer = {
    val healthStatusRef = new AtomicReference[HealthStatus](Unhealthy)

    val healthReporter: ReportsHealth = () => healthStatusRef.get()

    new RecoveringIndexer(
      scheduler = scheduler,
      executionContext = executionContext,
      restartDelay = restartDelay,
      updateHealthStatus = healthStatusRef.set,
      healthReporter = healthReporter,
    )
  }
}
