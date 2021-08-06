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
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.configuration.LedgerConfigurationSubscriptionFromIndex._

import scala.concurrent.duration.{Duration, DurationInt, DurationLong}
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Subscribes to ledger configuration updates coming from the index, and makes the latest ledger
  * configuration available to consumers.
  *
  * This class helps avoiding code duplication and limiting the number of database lookups, as
  * multiple services and validators require the latest ledger configuration.
  *
  * The [[subscription]] method returns a [[ResourceOwner]] which succeeds after the first lookup,
  * regardless of whether the index contains a configuration or not. It then starts a background
  * stream of configuration updates to keep its internal configuration state in sync with the index.
  *
  * The [[Subscription.ready]] method returns a [[Future]] that will not resolve until a
  * configuration is found or the load timeout expires.
  */
private[apiserver] final class LedgerConfigurationSubscriptionFromIndex(
    indexService: IndexConfigManagementService,
    scheduler: Scheduler,
    materializer: Materializer,
    servicesExecutionContext: ExecutionContext,
) {

  private val logger = ContextualizedLogger.get(getClass)

  def subscription(
      configurationLoadTimeout: Duration
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerConfigurationSubscription with IsReady] =
    new ResourceOwner[LedgerConfigurationSubscription with IsReady] {
      override def acquire()(implicit
          context: ResourceContext
      ): Resource[LedgerConfigurationSubscription with IsReady] =
        Resource(
          indexService
            .lookupConfiguration()
            .map { startingConfiguration =>
              new Subscription(startingConfiguration, configurationLoadTimeout)
            }(context.executionContext)
        )(subscription =>
          Future {
            subscription.stop()
          }(context.executionContext)
        )
    }

  private final class Subscription(
      startingConfiguration: Option[(LedgerOffset.Absolute, Configuration)],
      configurationLoadTimeout: Duration,
  )(implicit loggingContext: LoggingContext)
      extends LedgerConfigurationSubscription
      with IsReady {
    private val readyPromise = Promise[Unit]()

    private val (latestConfigurationState, scheduledTimeout) = startingConfiguration match {
      case Some((offset, configuration)) =>
        logger.info(
          s"Initial ledger configuration lookup found configuration $configuration at $offset. Looking for new ledger configurations from this offset."
        )
        readyPromise.trySuccess(())
        val configurationState: StateType = Some(offset) -> Some(configuration)
        (new AtomicReference[StateType](configurationState), Cancellable.alreadyCancelled)
      case None =>
        logger.info(
          s"Initial ledger configuration lookup did not find any configuration. Looking for new ledger configurations from the ledger beginning."
        )
        val configurationState: StateType = None -> None
        val scheduledTimeout = scheduler.scheduleOnce(
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
        (new AtomicReference[StateType](configurationState), scheduledTimeout)
    }

    private val killSwitch = RestartSource
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
              scheduledTimeout.cancel()
              latestConfigurationState.set(Some(offset) -> Some(config))
              readyPromise.trySuccess(())
              ()
            case (offset, domain.ConfigurationEntry.Rejected(_, _, _)) =>
              logger.info(s"New ledger configuration rejection found at $offset")
              latestConfigurationState.updateAndGet(previous => Some(offset) -> previous._2)
              ()
          }
      }
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.left[UniqueKillSwitch, Future[Done]])
      .run()(materializer)

    override def ready: Future[Unit] = readyPromise.future

    override def latestConfiguration(): Option[Configuration] = latestConfigurationState.get._2

    def stop(): Unit = {
      scheduledTimeout.cancel()
      killSwitch.shutdown()
    }
  }
}

private[apiserver] object LedgerConfigurationSubscriptionFromIndex {
  private type StateType = (Option[LedgerOffset.Absolute], Option[Configuration])

  trait IsReady {
    def ready: Future[Unit]
  }
}
