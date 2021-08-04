// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import akka.actor.Scheduler
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.apiserver.configuration.LedgerConfigurationProvisioner._
import com.daml.platform.configuration.InitialLedgerConfiguration
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}

import scala.compat.java8.FutureConverters
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationLong
import scala.util.{Failure, Success}

/** Writes a default ledger configuration to the ledger, after a configurable delay. The
  * configuration is only written if the ledger does not already have a configuration.
  *
  * Used by the participant to initialize a new ledger.
  *
  * Designed to be used only through the [[owner]] constructor.
  */
final class LedgerConfigurationProvisioner private (
    ledgerConfigurationSubscription: LedgerConfigurationSubscription,
    writeService: state.WriteConfigService,
    timeProvider: TimeProvider,
    submissionIdGenerator: SubmissionIdGenerator,
) {

  /** Submits the initial configuration immediately.
    * The [[owner]] constructor will schedule this with a delay.
    *
    * There are several reasons why the change could be rejected:
    *
    *   - The participant is not authorized to set the configuration
    *   - There already is a configuration, it just didn't appear in the index yet
    *
    * This method therefore does not try to re-submit the initial configuration in case of failure.
    */
  def submitInitialConfig(
      initialConfiguration: Configuration
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Unit =
    if (ledgerConfigurationSubscription.latestConfiguration().isEmpty) {
      val submissionId = submissionIdGenerator.generate()
      withEnrichedLoggingContext("submissionId" -> submissionId) { implicit loggingContext =>
        logger.info("No ledger configuration found, submitting an initial configuration.")
        DefaultTelemetry
          .runFutureInSpan(
            SpanName.LedgerConfigProviderInitialConfig,
            SpanKind.Internal,
          ) { implicit telemetryContext =>
            val maxRecordTime =
              Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60))
            FutureConverters.toScala(
              writeService.submitConfiguration(maxRecordTime, submissionId, initialConfiguration)
            )
          }
          .onComplete {
            case Success(state.SubmissionResult.Acknowledged) =>
              logger.info("Initial configuration submission was successful.")
            case Success(result: state.SubmissionResult.SynchronousError) =>
              withEnrichedLoggingContext("error" -> result) { implicit loggingContext =>
                logger.warn("Initial configuration submission failed.")
              }
            case Failure(exception) =>
              logger.error("Initial configuration submission failed.", exception)
          }
      }
    }
}

object LedgerConfigurationProvisioner {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      initialLedgerConfiguration: InitialLedgerConfiguration,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      writeService: state.WriteConfigService,
      timeProvider: TimeProvider,
      submissionIdGenerator: SubmissionIdGenerator,
      scheduler: Scheduler,
      servicesExecutionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): ResourceOwner[Unit] = {
    implicit val executionContext: ExecutionContext = servicesExecutionContext
    val provisioner = new LedgerConfigurationProvisioner(
      ledgerConfigurationSubscription,
      writeService,
      timeProvider,
      submissionIdGenerator,
    )
    ResourceOwner
      .forCancellable(() =>
        scheduler.scheduleOnce(
          initialLedgerConfiguration.delayBeforeSubmitting.toNanos.nanos,
          new Runnable {
            override def run(): Unit = {
              provisioner.submitInitialConfig(initialLedgerConfiguration.configuration)
            }
          },
        )
      )
      .map(_ => ())
  }
}
