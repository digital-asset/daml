// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.concurrent.atomic.AtomicReference

import akka.stream.Materializer
import akka.stream.scaladsl.{RestartSource, Sink}
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.v1.Configuration
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.configuration.LedgerConfiguration

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{DurationInt, DurationLong}

/**
  * This class subscribes to ledger configuration updates coming from the index,
  * and makes the latest ledger configuration available to consumers.
  *
  * This class is useful to avoid code duplication and to limit the number of
  * database lookups, as multiple services and validators require the latest ledger config.
  */
final class LedgerConfigProvider private (
    index: IndexConfigManagementService,
    config: LedgerConfiguration,
    materializer: Materializer,
)(implicit logCtx: LoggingContext) {

  // The latest offset that was read (if any), and the latest ledger configuration found (if any)
  private[this] type StateType = (Option[LedgerOffset.Absolute], Option[Configuration])
  private[this] val state: AtomicReference[StateType] = new AtomicReference(None -> None)

  // Promise for when the ledger configuration is ready
  private[this] val readyPromise: Promise[Unit] = Promise()
  materializer.scheduleOnce(config.configurationLoadTimeout.toNanos.nanos, () => {
    readyPromise.trySuccess(())
    ()
  })

  private[this] val logger = ContextualizedLogger.get(this.getClass)

  // At startup, look up the latest ledger configuration, then subscribe to a
  // stream of configuration changes.
  // If the source of configuration changes proves to be a performance bottleneck,
  // it could be replaced by regular polling.
  index
    .lookupConfiguration()
    .map {
      case Some(result) =>
        logger.info(
          s"Initial ledger configuration lookup found configuration ${result._2} at ${result._1}")
        configFound(result._1, result._2)
      case None =>
        logger.info(s"Initial ledger configuration lookup did not find any configuration")
        state.set(None -> None)
    }(DE)
    .map(_ => startStreamingUpdates())(DE)

  private[this] def configFound(offset: LedgerOffset.Absolute, config: Configuration): Unit = {
    state.set(Some(offset) -> Some(config))
    readyPromise.trySuccess(())
    ()
  }

  private[this] def startStreamingUpdates(): Unit = {
    RestartSource
      .withBackoff(
        minBackoff = 1.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.1,
      ) { () =>
        index
          .configurationEntries(state.get._1)
          .map {
            case (offset, domain.ConfigurationEntry.Accepted(_, _, config)) =>
              logger.info(s"New ledger configuration $config found at $offset")
              configFound(offset, config)
            case (offset, domain.ConfigurationEntry.Rejected(_, _, _, _)) =>
              logger.trace(s"New ledger configuration rejection found at $offset")
              state.updateAndGet(previous => Some(offset) -> previous._2)
              ()
          }
      }
      .runWith(Sink.ignore)(materializer)
    ()
  }

  /** The latest configuration found so far.
    * This may not be the currently active ledger configuration, e.g., if the index is lagging behind the ledger.
    */
  def latestConfiguration: Option[Configuration] = state.get._2

  /** Completes:
    * - when some ledger configuration was found
    * - after [[com.daml.platform.configuration.LedgerConfiguration.configurationLoadTimeout]]
    * , whichever happens first.
    */
  def ready: Future[Unit] = readyPromise.future
}

object LedgerConfigProvider {

  def create(index: IndexConfigManagementService, config: LedgerConfiguration)(
      implicit materializer: Materializer,
      logCtx: LoggingContext): LedgerConfigProvider =
    new LedgerConfigProvider(index, config, materializer)
}
