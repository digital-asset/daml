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
  * multiple services and validators require the latest ledger config.
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
        Resource(Future {
          new Subscription(configurationLoadTimeout)
        }(context.executionContext))(subscription =>
          Future {
            subscription.cancel()
            ()
          }(context.executionContext)
        )
    }

  private final class Subscription(
      configurationLoadTimeout: Duration
  )(implicit loggingContext: LoggingContext)
      extends LedgerConfigurationSubscription
      with IsReady
      with Cancellable {
    // The latest offset that was read (if any), and the latest ledger configuration found (if any)
    private val latestConfigurationState = new AtomicReference[StateType](None -> None)
    private val state = new AtomicReference[SubscriptionState](SubscriptionState.LookingUp)
    private val readyPromise = Promise[Unit]()

    private val scheduledTimeout = {
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

    // Looks up the latest ledger configuration, then subscribes to a stream of configuration changes.
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

    private def startStreamingUpdates(): Unit = {
      val killSwitch = RestartSource
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
        .run()(materializer)
      if (
        !state.compareAndSet(SubscriptionState.LookingUp, SubscriptionState.Streaming(killSwitch))
      ) {
        killSwitch.shutdown()
      }
      ()
    }

    private def configFound(
        offset: LedgerOffset.Absolute,
        config: Configuration,
    ): Unit = {
      scheduledTimeout.cancel()
      latestConfigurationState.set(Some(offset) -> Some(config))
      readyPromise.trySuccess(())
      ()
    }

    override def ready: Future[Unit] = readyPromise.future

    override def latestConfiguration(): Option[Configuration] = latestConfigurationState.get._2

    override def cancel(): Boolean = {
      scheduledTimeout.cancel()
      state.getAndSet(SubscriptionState.Stopped) match {
        case SubscriptionState.LookingUp =>
          true
        case SubscriptionState.Streaming(killSwitch) =>
          killSwitch.shutdown()
          true
        case SubscriptionState.Stopped =>
          false
      }
    }

    override def isCancelled: Boolean =
      state.get == SubscriptionState.Stopped
  }
}

private[apiserver] object LedgerConfigurationSubscriptionFromIndex {
  private type StateType = (Option[LedgerOffset.Absolute], Option[Configuration])

  private sealed trait SubscriptionState

  private object SubscriptionState {
    case object LookingUp extends SubscriptionState

    final case class Streaming(killSwitch: UniqueKillSwitch) extends SubscriptionState

    case object Stopped extends SubscriptionState
  }

  trait IsReady {
    def ready: Future[Unit]
  }
}
