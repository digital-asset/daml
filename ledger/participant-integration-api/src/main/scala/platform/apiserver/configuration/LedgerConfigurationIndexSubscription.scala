// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{Cancellable, Scheduler}
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.duration.{Duration, DurationInt, DurationLong}
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Subscribes to ledger configuration updates coming from the index, and makes the latest ledger
  * configuration available to consumers.
  *
  * This class helps avoiding code duplication and limiting the number of database lookups, as
  * multiple services and validators require the latest ledger config.
  */
private[apiserver] final class LedgerConfigurationIndexSubscription private (
    configurationLoadTimeout: Duration,
    indexService: IndexConfigManagementService,
    scheduler: Scheduler,
    servicesExecutionContext: ExecutionContext,
)(implicit
    materializer: Materializer,
    loggingContext: LoggingContext,
) extends LedgerConfigurationSubscription
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  // The latest offset that was read (if any), and the latest ledger configuration found (if any)
  private type StateType = (Option[LedgerOffset.Absolute], Option[Configuration])
  private val latestConfigurationState = new AtomicReference[StateType](None -> None)
  private val killSwitch = new AtomicReference[Option[UniqueKillSwitch]](None)
  private val readyPromise = Promise[Unit]()

  // At startup, do the following:
  // - Start loading the ledger configuration
  // - Submit the initial config if none is found after a delay
  // - Mark the provider as ready if no configuration was found after a timeout
  private val scheduledTimeout = giveUpAfterTimeout()
  startLoading()

  // Looks up the latest ledger configuration, then subscribes to a
  // stream of configuration changes.
  // If the source of configuration changes proves to be a performance bottleneck,
  // it could be replaced by regular polling.
  private[this] def startLoading(): Unit =
    indexService
      .lookupConfiguration()
      .map {
        case Some(result) =>
          logger.info(
            s"Initial ledger configuration lookup found configuration ${result._2} at ${result._1}. Looking for new ledger configurations from this offset."
          )
          configFound(result._1, result._2)
        case None =>
          logger.info(
            s"Initial ledger configuration lookup did not find any configuration. Looking for new ledger configurations from the ledger beginning."
          )
          latestConfigurationState.set(None -> None)
      }(servicesExecutionContext)
      .map(_ => startStreamingUpdates())(servicesExecutionContext)
      .failed
      .foreach { exception =>
        readyPromise.tryFailure(exception)
        logger.error("Could not load the ledger configuration.", exception)
      }(servicesExecutionContext)

  private[this] def configFound(
      offset: LedgerOffset.Absolute,
      config: Configuration,
  ): Unit = {
    scheduledTimeout.cancel()
    latestConfigurationState.set(Some(offset) -> Some(config))
    readyPromise.trySuccess(())
    ()
  }

  private[this] def startStreamingUpdates(): Unit = {
    killSwitch.set(
      Some(
        RestartSource
          .withBackoff(
            RestartSettings(
              minBackoff = 1.seconds,
              maxBackoff = 30.seconds,
              randomFactor = 0.1,
            )
          ) { () =>
            indexService
              .configurationEntries(latestConfigurationState.get._1)
              .map {
                case (offset, domain.ConfigurationEntry.Accepted(_, config)) =>
                  logger.info(s"New ledger configuration $config found at $offset")
                  configFound(offset, config)
                case (offset, domain.ConfigurationEntry.Rejected(_, _, _)) =>
                  logger.info(s"New ledger configuration rejection found at $offset")
                  latestConfigurationState.updateAndGet(previous => Some(offset) -> previous._2)
                  ()
              }
          }
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .toMat(Sink.ignore)(Keep.left[UniqueKillSwitch, Future[Done]])
          .run()
      )
    )
    ()
  }

  private def giveUpAfterTimeout(): Cancellable = {
    scheduler.scheduleOnce(
      configurationLoadTimeout.toNanos.nanos,
      new Runnable {
        override def run(): Unit = {
          if (readyPromise.trySuccess(())) {
            logger.warn(
              s"No ledger configuration found after $configurationLoadTimeout. The ledger API server will now start but all services that depend on the ledger configuration will return UNAVAILABLE until at least one ledger configuration is found."
            )
          }
          ()
        }
      },
    )(servicesExecutionContext)
  }

  /** This future will resolve successfully:
    *
    *   - when a ledger configuration is found, or
    *   - after [[configurationLoadTimeout]].
    *
    * Whichever is first.
    */
  def ready: Future[Unit] = readyPromise.future

  override def latestConfiguration: Option[Configuration] = latestConfigurationState.get._2

  override def close(): Unit = {
    scheduledTimeout.cancel()
    killSwitch.get.foreach(k => k.shutdown())
  }
}

private[apiserver] object LedgerConfigurationIndexSubscription {
  def owner(
      configurationLoadTimeout: Duration,
      indexService: IndexConfigManagementService,
      scheduler: Scheduler,
      materializer: Materializer,
      servicesExecutionContext: ExecutionContext,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerConfigurationIndexSubscription] =
    ResourceOwner.forCloseable(() =>
      new LedgerConfigurationIndexSubscription(
        configurationLoadTimeout,
        indexService,
        scheduler,
        servicesExecutionContext,
      )(materializer, loggingContext)
    )
}
