// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import akka.actor.{Cancellable, Scheduler}
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.domain.LedgerOffset
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.configuration.LedgerConfigurationSubscriptionFromIndex.*

import java.util.concurrent.atomic.AtomicReference
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
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def subscription(
      configurationLoadTimeout: Duration
  )(implicit
      loggingContext: LoggingContextWithTrace
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
        )(subscription => subscription.stop())
    }

  private final class Subscription(
      startingConfiguration: Option[(LedgerOffset.Absolute, Configuration)],
      configurationLoadTimeout: Duration,
  )(implicit loggingContext: LoggingContextWithTrace)
      extends LedgerConfigurationSubscription
      with IsReady {
    private val readyPromise = Promise[Unit]()

    private val (currentState, scheduledTimeout) = startingConfiguration match {
      case Some((offset, configuration)) =>
        logger.info(
          s"Initial ledger configuration lookup found configuration $configuration at $offset. Looking for new ledger configurations from this offset."
        )
        readyPromise.trySuccess(()).discard
        val state = State.ConfigurationFound(offset, configuration)
        (new AtomicReference[State](state), Cancellable.alreadyCancelled)
      case None =>
        logger.info(
          s"Initial ledger configuration lookup did not find any configuration. Looking for new ledger configurations from the ledger beginning."
        )
        val state = State.NoConfiguration
        val scheduledTimeout = scheduler.scheduleOnce(
          configurationLoadTimeout.toNanos.nanos,
          new Runnable {
            override def run(): Unit = {
              if (readyPromise.trySuccess(())) {
                logger.warn(
                  s"No ledger configuration found after $configurationLoadTimeout. The ledger API server will now start " +
                    s"but all services that depend on the ledger configuration will return " +
                    s"${RequestValidationErrors.NotFound.LedgerConfiguration.id} until at least one ledger configuration is found."
                )
              }
              ()
            }
          },
        )(servicesExecutionContext)
        (new AtomicReference[State](state), scheduledTimeout)
    }

    private val (killSwitch, completionFuture) = RestartSource
      .withBackoff(
        RestartSettings(
          minBackoff = 1.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.1,
        )
      ) { () =>
        indexService
          .configurationEntries(currentState.get.latestOffset)
          .map {
            case (offset, domain.ConfigurationEntry.Accepted(_, configuration)) =>
              logger.info(s"New ledger configuration $configuration found at $offset")
              scheduledTimeout.cancel().discard
              currentState.set(State.ConfigurationFound(offset, configuration))
              readyPromise.trySuccess(()).discard
            case (offset, _: domain.ConfigurationEntry.Rejected) =>
              logger.info(s"New ledger configuration rejection found at $offset")
              // We only care about updating; we ignore the return value.
              currentState.updateAndGet {
                case State.NoConfiguration | State.OnlyRejectedConfigurations(_) =>
                  State.OnlyRejectedConfigurations(offset)
                case State.ConfigurationFound(_, configuration) =>
                  State.ConfigurationFound(offset, configuration)
              }
              ()
          }
      }
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()(materializer)

    override def ready: Future[Unit] = readyPromise.future

    override def latestConfiguration(): Option[Configuration] =
      currentState.get.latestConfiguration

    def stop(): Future[Unit] = {
      scheduledTimeout.cancel().discard
      killSwitch.shutdown()
      completionFuture.map(_ => ())(servicesExecutionContext)
    }
  }
}

private[apiserver] object LedgerConfigurationSubscriptionFromIndex {
  private sealed trait State {
    def latestOffset: Option[LedgerOffset.Absolute]

    def latestConfiguration: Option[Configuration]
  }

  private object State {
    case object NoConfiguration extends State {
      override val latestOffset: Option[LedgerOffset.Absolute] = None

      override val latestConfiguration: Option[Configuration] = None
    }

    final case class OnlyRejectedConfigurations(offset: LedgerOffset.Absolute) extends State {
      override val latestOffset: Option[LedgerOffset.Absolute] = Some(offset)

      override val latestConfiguration: Option[Configuration] = None
    }

    final case class ConfigurationFound(offset: LedgerOffset.Absolute, configuration: Configuration)
        extends State {
      override val latestOffset: Option[LedgerOffset.Absolute] = Some(offset)

      override val latestConfiguration: Option[Configuration] = Some(configuration)
    }
  }

  trait IsReady {
    def ready: Future[Unit]
  }
}
