// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.SubmissionSynchronizerNotReady
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.NoSynchronizerOnWhichAllSubmittersCanSubmit
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTestCommon
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Interpreter.ContractNotActive
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.{
  ContractNotFound,
  DuplicateContractKey,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  DvpTrader,
  Master,
  MasterDynamicConfig,
}
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.elements.DriverStatus
import com.digitalasset.canton.performance.model.java.orchestration.runtype
import com.digitalasset.canton.performance.{
  Connectivity,
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.{
  InactiveContracts,
  LockedContracts,
}
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import org.scalatest.Assertion
import org.scalatest.matchers.Matcher

import scala.concurrent.Await
import scala.concurrent.duration.*

trait ReliabilityPerformanceIntegrationTest extends BasePerformanceIntegrationTestCommon {

  /** how many commands should be submitted twice (0,1) */
  protected def duplicateSubmissionRatio: Double = 1.0 // we want lots of contention by default

  def checkPerfRunnerCompletion(index: Int): Unit =
    if (index <= 2)
      fail("Haven't crashed the node at least twice before the performance runner completed")
    else
      logger.debug(s"Ignore crash $index due to performance runner completion")

  protected def acceptableErrorOrWarning(entry: LogEntry): Assertion =
    entry.message should matchAcceptableErrorOrWarnMessage

  protected def messageIncludesOneOf(messages: String*): Matcher[String] =
    messages.map(include(_)).reduceOption(_ or _).value

  protected def matchAcceptableErrorOrWarnMessage: Matcher[String] =
    messageIncludesOneOf(
      InactiveContracts.id,
      ContractNotFound.id,
      LockedContracts.id,
      ContractNotActive.id,
      DuplicateContractKey.id,
      NoSynchronizerOnWhichAllSubmittersCanSubmit.id,
      NotConnectedToAnySynchronizer.id,
      SubmissionDuringShutdown.id,
      SubmissionSynchronizerNotReady.id,
      LostSequencerSubscription.id,
      "Now retrying operation 'subscribe'",
      "The operation 'subscribe' has failed with an exception",
      "Server is shutting down",
      "timed out at 20", // covers timeout logged by AbstractMessageProcessor and ProtocolProcessor
      "has exceeded the max-sequencing",
      "UNAVAILABLE",
      "Detected late processing (or clock skew) of batch",
      "failed with INTERNAL/head of empty stream",
      "failed with INTERNAL/sql-ledger: Dispatcher is closed",
      "UNKNOWN / missing GRPC status in response", // can happen during shutdown
      "searching for topology change delay",
      "periodic acknowledgement failed",
      "Locked connection was lost, trying to rebuild",
      "has not completed after",
      "did not complete within",
      "due to Duplicate",
      "Ledger subscription PerformanceRunner failed with an error",
      "The operation 'restartable-PerformanceRunner' has failed with an exception",
      "Now retrying operation 'restartable-PerformanceRunner'",
    ) or
      (include("Failed to submit") and
        (
          include("DEADLINE_EXCEEDED")
          // in some rare cases, we receive a RST_STREAM error (either during command submission or on our subscription)
          // during shutdown of the node
            or include("RST_STREAM closed stream. HTTP/2 error code: CANCEL")
            or include("RST_STREAM closed stream. HTTP/2 error code: REFUSED_STREAM")
        ))

  protected def runWithFault(
      participantName: String,
      ledgerApiPort: Port,
      withPartyGrowth: Int = 0,
      totalCycles: Int = 300,
      numAssetsPerIssuer: Int = 100,
  )(
      introduceFailure: (() => Boolean) => Unit
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val settings = new RateSettings(
      SubmissionRateSettings.TargetLatency(
        // introduce contention by turning off pending tracking
        duplicateSubmissionRatio = duplicateSubmissionRatio
      )
    )
    val config = PerformanceRunnerConfig(
      master = s"RestartMaster",
      localRoles = Set(
        Master(
          s"RestartMaster",
          quorumParticipants = 3,
          quorumIssuers = 2,
          runConfig = MasterDynamicConfig(
            totalCycles = totalCycles,
            reportFrequency = 10,
            runType = new runtype.DvpRun(
              numAssetsPerIssuer.toLong,
              0,
              0,
              withPartyGrowth.toLong,
            ),
          ),
        ),
        DvpTrader(s"RestartTradie1", settings = settings),
        DvpTrader(s"RestartTradie2", settings = settings),
        DvpTrader(s"RestartTradie3", settings = settings),
        DvpIssuer(s"RestartIssuator1", settings = RateSettings.defaults),
        DvpIssuer(s"RestartIssuator2", settings = RateSettings.defaults),
      ),
      ledger = Connectivity(name = participantName, port = ledgerApiPort),
      baseSynchronizerId = daId,
    )

    val runner =
      new PerformanceRunner(
        config,
        _ => NoOpMetricsFactory,
        loggerFactory.append(participantName, participantName),
      )
    env.environment.addUserCloseable(runner)

    val runnerStartup = runner.startup()

    logger.info("Starting runner")

    // wait until we progressed a bit
    eventually(timeUntilSuccess = 200.seconds) {
      val runnerStatus = runner.status()
      runnerStatus.exists {
        case traderStatus: DriverStatus.TraderStatus => traderStatus.proposals.observed > 10
        case _ => false
      } shouldBe true withClue s"Status did not have enough observed proposals: $runnerStatus"
    }

    logger.info("Sufficiently many proposals have been created, progressing")

    val fut = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        introduceFailure(() => runnerStartup.isCompleted)
        runner.updateRateSettings(_.copy(SubmissionRateSettings.TargetLatency(startRate = 3)))
        logger.info("Waiting for runner startup to complete")
        for {
          _ <- runnerStartup
          _ = logger.info("Runner started ; closing the runner")
          _ <- runner.closeF()
        } yield ()
      },
      seq => forAll(seq)(acceptableErrorOrWarning),
    )
    Await.result(fut, 1000.seconds)
  }
}
