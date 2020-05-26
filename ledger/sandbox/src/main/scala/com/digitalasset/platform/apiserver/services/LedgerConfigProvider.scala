// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.{Done, NotUsed}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import com.daml.api.util.TimeProvider
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmissionId,
  SubmissionResult,
  WriteService
}
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.configuration.LedgerConfiguration

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{DurationInt, DurationLong}

/**
  * Subscribes to ledger configuration updates coming from the index,
  * and makes the latest ledger configuration available to consumers.
  *
  * This class helps avoiding code duplication and limiting the number of
  * database lookups, as multiple services and validators require the latest ledger config.
  */
final class LedgerConfigProvider private (
    index: IndexConfigManagementService,
    writeService: WriteService,
    timeProvider: TimeProvider,
    config: LedgerConfiguration,
    materializer: Materializer,
)(implicit logCtx: LoggingContext)
    extends AutoCloseable {

  private[this] val logger = ContextualizedLogger.get(this.getClass)

  // The latest offset that was read (if any), and the latest ledger configuration found (if any)
  private[this] type StateType = (Option[LedgerOffset.Absolute], Option[Configuration])
  private[this] val state: AtomicReference[StateType] = new AtomicReference(None -> None)
  private[this] val killSwitch: AtomicReference[Option[UniqueKillSwitch]] = new AtomicReference(
    None)
  private[this] val readyPromise: Promise[Unit] = Promise()

  // At startup, do the following:
  // - Start loading the ledger configuration
  // - Mark the provider as ready if no configuration was found after a timeout
  // - Submit the initial config if none is found after a delay
  startLoading()
  materializer.scheduleOnce(config.configurationLoadTimeout.toNanos.nanos, () => {
    readyPromise.trySuccess(())
    ()
  })
  materializer.scheduleOnce(config.initialConfigurationSubmitDelay.toNanos.nanos, () => {
    if (latestConfiguration.isEmpty) submitInitialConfig()
    ()
  })

  // Looks up the latest ledger configuration, then subscribes to a
  // stream of configuration changes.
  // If the source of configuration changes proves to be a performance bottleneck,
  // it could be replaced by regular polling.
  private[this] def startLoading(): Future[Unit] =
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
    killSwitch.set(
      Some(
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
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .toMat(Sink.ignore)(Keep.left[UniqueKillSwitch, Future[Done]])
          .run()(materializer)))
    ()
  }

  private[this] def submitInitialConfig(): Future[Unit] = {
    implicit val executionContext: ExecutionContext = DE
    // There are several reasons why the change could be rejected:
    // - The participant is not authorized to set the configuration
    // - There already is a configuration, it just didn't appear in the index yet
    // This method therefore does not try to re-submit the initial configuration in case of failure.
    val submissionId = SubmissionId.assertFromString(UUID.randomUUID.toString)
    logger.info(s"No ledger configuration found, submitting an initial configuration $submissionId")
    FutureConverters
      .toScala(
        writeService.submitConfiguration(
          Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60)),
          submissionId,
          config.initialConfiguration
        ))
      .map {
        case SubmissionResult.Acknowledged =>
          logger.info(s"Initial configuration submission $submissionId was successful")
          ()
        case SubmissionResult.NotSupported =>
          logger.info("Setting an initial ledger configuration is not supported")
          ()
        case result =>
          logger.warn(
            s"Initial configuration submission $submissionId failed. Reason: ${result.description}")
          ()
      }(DE)
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

  override def close(): Unit = {
    killSwitch.get.foreach(k => k.shutdown())
  }
}

object LedgerConfigProvider {

  def create(
      index: IndexConfigManagementService,
      writeService: WriteService,
      timeProvider: TimeProvider,
      config: LedgerConfiguration)(
      implicit materializer: Materializer,
      logCtx: LoggingContext): LedgerConfigProvider =
    new LedgerConfigProvider(index, writeService, timeProvider, config, materializer)
}
