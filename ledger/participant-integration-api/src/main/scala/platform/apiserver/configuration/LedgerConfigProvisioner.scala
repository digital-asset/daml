// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Scheduler
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.DurationLong
import scala.concurrent.{ExecutionContext, Future}

/** Writes a default ledger configuration to the ledger, after a configurable delay. The
  * configuration is only written if the ledger does not already have a configuration.
  *
  * Used by the participant to initialize a new ledger.
  */
private[apiserver] final class LedgerConfigProvisioner private (
    ledgerConfiguration: LedgerConfiguration,
    currentLedgerConfiguration: CurrentLedgerConfiguration,
    writeService: state.WriteConfigService,
    timeProvider: TimeProvider,
    submissionIdGenerator: SubmissionIdGenerator,
    scheduler: Scheduler,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends AutoCloseable {
  private val logger = ContextualizedLogger.get(getClass)

  private val closed: AtomicBoolean = new AtomicBoolean(false)

  scheduler.scheduleOnce(
    ledgerConfiguration.initialConfigurationSubmitDelay.toNanos.nanos,
    new Runnable {
      override def run(): Unit = {
        if (currentLedgerConfiguration.latestConfiguration.isEmpty && !closed.get)
          submitInitialConfig(writeService)
        ()
      }
    },
  )

  // There are several reasons why the change could be rejected:
  // - The participant is not authorized to set the configuration
  // - There already is a configuration, it just didn't appear in the index yet
  // This method therefore does not try to re-submit the initial configuration in case of failure.
  private def submitInitialConfig(writeService: state.WriteConfigService): Future[Unit] = {
    val submissionId = submissionIdGenerator.generate()
    withEnrichedLoggingContext("submissionId" -> submissionId) { implicit loggingContext =>
      logger.info(s"No ledger configuration found, submitting an initial configuration.")
      DefaultTelemetry.runFutureInSpan(
        SpanName.LedgerConfigProviderInitialConfig,
        SpanKind.Internal,
      ) { implicit telemetryContext =>
        FutureConverters
          .toScala(
            writeService.submitConfiguration(
              Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60)),
              submissionId,
              ledgerConfiguration.initialConfiguration,
            )
          )
          .map {
            case state.SubmissionResult.Acknowledged =>
              logger.info(s"Initial configuration submission was successful.")
            case result: state.SubmissionResult.SynchronousError =>
              withEnrichedLoggingContext("error" -> result) { implicit loggingContext =>
                logger.warn(s"Initial configuration submission failed.")
              }
          }
      }
    }
  }

  override def close(): Unit = {
    closed.set(true)
  }
}

object LedgerConfigProvisioner {
  def owner(
      ledgerConfiguration: LedgerConfiguration,
      currentLedgerConfiguration: CurrentLedgerConfiguration,
      writeService: state.WriteConfigService,
      timeProvider: TimeProvider,
      submissionIdGenerator: SubmissionIdGenerator,
      scheduler: Scheduler,
      executionContext: ExecutionContext,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerConfigProvisioner] =
    ResourceOwner.forCloseable(() =>
      new LedgerConfigProvisioner(
        ledgerConfiguration,
        currentLedgerConfiguration,
        writeService,
        timeProvider,
        submissionIdGenerator,
        scheduler,
      )(executionContext, loggingContext)
    )
}
