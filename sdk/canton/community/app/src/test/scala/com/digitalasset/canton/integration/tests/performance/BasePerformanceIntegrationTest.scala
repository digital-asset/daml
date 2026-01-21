// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.digitalasset.base.error.ErrorCategory
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.{DbConfig, NonNegativeDuration}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.error.ErrorCodeUtils.errorCategoryFromString
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseConfigTransforms, UseReferenceBlockSequencer}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateContractKey
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.performance.*
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  DvpTrader,
  Master,
  MasterDynamicConfig,
}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.model.java.orchestration.runtype
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.InactiveContracts
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.topology.SynchronizerId
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

trait BasePerformanceIntegrationTest extends BasePerformanceIntegrationTestCommon {

  protected lazy val baseEnvironmentConfig: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.addConfigTransforms(
      _.focus(_.parameters.timeouts.processing.shutdownProcessing)
        .replace(NonNegativeDuration.tryFromDuration(1.minute)),
      // Consistency checks are slow, so we should not use them in performance tests
      _.focus(_.parameters.enableAdditionalConsistencyChecks).replace(false),
    )

  // Apply the storage queue size config transform after the storage plugin has modified the config
  protected lazy val storageQueuePlugin: EnvironmentSetupPlugin =
    new UseConfigTransforms(Seq(ConfigTransforms.setStorageQueueSize(10000)), loggerFactory)

  protected def setupPlugins(storagePlugin: EnvironmentSetupPlugin): Unit = {
    registerPlugin(storagePlugin)
    registerPlugin(storageQueuePlugin)
    registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  }

  /** Override to customize the setup the topology. Custom implementations should ensure that
    * participant1 and participant2 are connected to the da synchronizer.
    */
  protected def baseSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participant1_.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
  }

  protected def performanceTestSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Deploying packages locally, because remote deployment would not wait for packages becoming ready.
    Seq(participant1_, participant2).foreach(_.dars.upload(BaseTest.PerformanceTestPath))
  }

  // we use a bit of indirection here to allow implementators to overhaul aspects of the environment setup
  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentConfig
      .withSetup { implicit env =>
        baseSetup
        performanceTestSetup
      }
}

object BasePerformanceIntegrationTest {
  def defaultConfigs(
      index: Int,
      participant1: LocalParticipantReference,
      participant2: LocalParticipantReference,
      reportFrequency: Int = 6,
      totalCycles: Int = 30,
      numAssetsPerIssuer: Int = 20,
      withPartyGrowth: Int = 0,
      rateSettings: RateSettings,
      otherSynchronizers: Seq[SynchronizerId] = Nil,
      otherSynchronizersRatio: Double = 0.0,
  )(implicit
      env: TestConsoleEnvironment
  ): (PerformanceRunnerConfig, PerformanceRunnerConfig) = (
    PerformanceRunnerConfig(
      master = s"${index}Master",
      localRoles = Set(
        Master(
          s"${index}Master",
          runConfig = MasterDynamicConfig(
            totalCycles = totalCycles,
            reportFrequency = reportFrequency,
            runType = new runtype.DvpRun(
              numAssetsPerIssuer.toLong,
              0,
              0,
              withPartyGrowth.toLong,
            ),
          ),
        ),
        DvpTrader(s"${index}Tradie", rateSettings),
        DvpIssuer(s"${index}Issuator", rateSettings),
      ),
      ledger = toConnectivity(participant1),
      baseSynchronizerId = env.daId.logical,
      otherSynchronizers = otherSynchronizers,
      otherSynchronizersRatio = otherSynchronizersRatio,
    ),
    PerformanceRunnerConfig(
      master = s"${index}Master",
      localRoles = Set(
        DvpTrader(s"${index}John", rateSettings),
        DvpIssuer(s"${index}Cleese", rateSettings),
        DvpTrader(s"${index}Benny", rateSettings),
      ),
      ledger = toConnectivity(participant2),
      baseSynchronizerId = env.daId.logical,
      otherSynchronizers = otherSynchronizers,
    ),
  )

  def toConnectivity(
      p: LocalParticipantReference
  ): Connectivity =
    Connectivity(
      name = p.name,
      port = p.config.ledgerApi.port,
      tls = p.config.ledgerApi.tls.map(_.clientConfig),
    )
}

trait BasePerformanceIntegrationTestCommon
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  protected lazy val waitTimeout: NonNegativeDuration = NonNegativeDuration(1000.seconds)

  protected def waitUntilComplete(
      first: (PerformanceRunner, Future[Either[String, Unit]]),
      snd: (PerformanceRunner, Future[Either[String, Unit]]),
  )(implicit executionContext: ExecutionContext): Unit = {

    val (_runner1, f1) = first
    val (_runner2, f2) = snd

    waitTimeout.await("waitUntilComplete")(for {
      _ <- f1
      _ <- f2
    } yield {})

  }

  // completely overload the system on startup
  protected def whackOnStartup(config: PerformanceRunnerConfig): PerformanceRunnerConfig = {
    val whackMe = RateSettings(SubmissionRateSettings.TargetLatency(startRate = 20))
    config
      .focus(_.localRoles)
      .replace(config.localRoles.map {
        case role: ActivePartyRole =>
          role match {
            case x: DvpTrader => x.focus(_.settings).replace(whackMe)
            case x: DvpIssuer => x
          }
        case m => m
      })
  }

  protected def acceptableLogMessage(entry: LogEntry): Assertion =
    acceptableLogMessageExt(Seq(), Seq())(entry)

  protected def acceptableLogMessageExt(
      additional: Seq[String],
      additionalNonRetryable: Seq[String],
  )(entry: LogEntry): Assertion =
    errorCategoryFromString(entry.message) match {
      case Some(category)
          if category.retryable.nonEmpty || category == ErrorCategory.BackgroundProcessDegradationWarning =>
        succeed

      case Some(other) =>
        if (additionalNonRetryable.isEmpty) {
          fail(s"unexpected non-retryable ($other) log entry $entry")
        } else {
          exists(Table("Expected non-retryable warning", additionalNonRetryable*)) { (x: String) =>
            entry.message should include(x)
          }
        }
      case _ =>
        val valid = Seq(
          "Detected late processing (or clock skew)",
          "timed out at",
          "DEADLINE_EXCEEDED",
          InactiveContracts.id,
          DuplicateContractKey.id,
          SubmissionRequestRefused.id,
          "has not completed after",
          "Sequencing result message timed out",
          // ignore slow query warnings
          "Slow database query",
          "Very slow or blocked queries detected",
          // TODO(#11240) remove me once the issue is fixed (flaky fix for ChangeScenarioPerformanceIntegrationTest)
          "failed to allocate bystander party",
          "Test run orchestration initialisation failed. Retrying",
        ) ++ additional
        exists(Table("Expected message", valid*)) { (x: String) =>
          entry.message should include(x)
        }
    }
}
