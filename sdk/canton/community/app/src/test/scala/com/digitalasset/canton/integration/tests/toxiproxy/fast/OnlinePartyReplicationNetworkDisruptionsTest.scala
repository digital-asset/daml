// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.multihostedparties.OnlinePartyReplicationTestHelpers
import com.digitalasset.canton.integration.tests.toxiproxy.{
  ToxiproxyHelpers,
  ToxiproxyParticipantSynchronizerHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import eu.rekawek.toxiproxy.model.{Toxic, ToxicDirection}
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import scala.collection.concurrent.TrieMap
import scala.concurrent.Promise

/** Objective: Ensure OnPR is resilient against network disruptions using toxiproxy.
  *
  * Setup:
  *   - 2 participants: each hosting parties to replicate to the other participant.
  *   - 1 sequencer hosting channel, 1 mediator both used for regular canton transaction processing.
  */
sealed trait OnlinePartyReplicationNetworkDisruptionsTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with SharedEnvironment {

  val toxiproxy = new UseToxiproxy(ToxiproxyConfig(List(proxyConf())))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiproxy)

  // TODO(#26775): Remove hard-coding of ProtocolVersion.dev (done as toxiproxy tests don't run pv.dev otherwise)
  private val devProtocolVersionToRemove = ProtocolVersion.dev
  override lazy val testedProtocolVersion: ProtocolVersion = devProtocolVersionToRemove

  // Test scenarios used for custom parties mapping to the scenario source participant
  private val testScenarios = Map(
    "timeout-sp-upstream" -> "participant1",
    "timeout-sp-downstream" -> "participant1",
    "timeout-tp-upstream" -> "participant2",
    // TODO(#27175): reenable - see below: "timeout-tp-downstream" -> "participant2",
  )
  // Hold customer parties once they are created in setup
  private val scenariosToParties = new TrieMap[String, (PartyId, PartyId)]()
  private var scenariosActuallyRun = Set.empty[String]

  // Test interceptor config
  // The threshold after which to inject the network disruption while OnPR is actively running.
  private val lowerContractCountToUnblockDisruption = 10
  // In the less likely event that OnPR progresses unusually fast and reaches this higher threshold,
  // pause OnPR to ensure the network disruption happens before OnPR has completed.
  private val largerContractCountToPauseOnPR = 100

  // Test interceptor state
  private var isReadyForDisruption: Promise[Unit] = _
  private var hasOnPRBeenDisruptedOrPaused: Boolean = false
  private var hasDisruptionBeenFixed: Boolean = false

  // Use the test interceptor to block OnPR after replication has started, but before the disruption
  // has been fixed.
  private def createSourceParticipantTestInterceptor() =
    PartyReplicationTestInterceptorImpl.sourceParticipantProceedsIf { stateSP =>
      // Once OnPR has been paused, wait until the disruption has been lifted unblocking OnPR.
      val result =
        if (hasOnPRBeenDisruptedOrPaused) hasDisruptionBeenFixed
        else {
          // Otherwise pause once a minimum number of contracts have been sent to the TP
          // to ensure the disruption (e.g. sequencer restart) happens when both participants are still busy
          // with OnPR. To make the disruption more realistic, use two thresholds: After the lower threshold,
          // allow the disruption (e.g. seq) to happen more realistically without pausing OnPR; if the second
          // threshold is reached (e.g. because OnPR runs unusually fast), pause OnPR.
          val contractCount = stateSP.sentContractsCount.unwrap
          if (contractCount > largerContractCountToPauseOnPR) {
            // It is less desirable to have to pause OnPR as that makes the test less aggressive or chaotic
            // as OnPR is in quiet and more deterministic state.
            logger.info(s"Pausing OnPR at $contractCount")
            hasOnPRBeenDisruptedOrPaused = true
          } else if (contractCount > lowerContractCountToUnblockDisruption) {
            // Instead we prefer for the test to interrupt the actively running OnPR, e.g. while receiving
            // or sending messages.
            if (!isReadyForDisruption.isCompleted) {
              logger.info(s"Ready for disrupting OnPR at $contractCount")
              isReadyForDisruption.success(())
            } else {
              logger.info(s"Already marked ready for disrupting OnPR at $contractCount")
            }
          }
          !hasOnPRBeenDisruptedOrPaused
        }
      result
    }

  protected def proxyConf(): ParticipantToSequencerPublicApi =
    ParticipantToSequencerPublicApi(
      sequencer = "sequencer1",
      name = "participant1-to-sequencer1-onpr",
    )

  protected def getProxy: RunningProxy = toxiproxy.runningToxiproxy.getProxy(proxyConf().name).value

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(
          EnvironmentDefinition.S1M1
            .focus(_.staticSynchronizerParameters.protocolVersion)
            .replace(devProtocolVersionToRemove)
        )
      }
      .addConfigTransforms(
        (ConfigTransforms.setProtocolVersion(devProtocolVersionToRemove) ++
          ConfigTransforms.unsafeEnableOnlinePartyReplication(
            Map(
              // configure the test interceptor on both participants since the tests vary the source participant
              "participant1" -> (() => createSourceParticipantTestInterceptor()),
              "participant2" -> (() => createSourceParticipantTestInterceptor()),
            )
          ))*
      )
      .withSetup { implicit env =>
        import env.*

        // More aggressive AcsCommitmentProcessor checking.
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = PositiveSeconds.tryOfSeconds(1).toConfig),
          )

        // Helper connects participant1 to sequencer1 via toxiproxy, but keeps the participant2 to sequencer1
        // connection intact. Some tests use (SP, TP) := (P1, P2) and others (SP, TP) := (P2, P1) to impose
        // network disruption on the SP and TP respectively.
        ToxiproxyParticipantSynchronizerHelpers.connectParticipantsToDa(proxyConf(), toxiproxy)

        participants.all.dars.upload(CantonExamplesPath)

        // Allocate the parties for all test scenarios and remember them for use by the individual tests below.
        testScenarios.foreach { case (scenario, participantName) =>
          val participant = participants.all
            .find(_.name == participantName)
            .getOrElse(fail(s"Participant $participantName not found"))
          val partyToReplicate = participant.parties.enable(s"$scenario-alice")
          val fellowContractStakeholder = participant.parties.enable(s"$scenario-bob")
          scenariosToParties.put(scenario, (partyToReplicate, fellowContractStakeholder)).discard
        }
      }
      .withTeardown(_ =>
        ToxiproxyHelpers.removeAllProxies(toxiproxy.runningToxiproxy.controllingToxiproxyClient)
      )

  private val numContractsInCreateBatch = PositiveInt.tryCreate(100)

  private def initiateOnPRAndInitializeTestInterceptor(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      partyToReplicate: PartyId,
      fellowContractStakeholder: PartyId,
  )(implicit env: integration.TestConsoleEnvironment): (String, PositiveInt) = {
    import env.*

    val onPRSetup = createSharedContractsAndProposeTopologyForOnPR(
      sourceParticipant,
      targetParticipant,
      partyToReplicate,
      fellowContractStakeholder,
      numContractsInCreateBatch,
    )

    // Reset the test interceptor for the next test.
    isReadyForDisruption = Promise[Unit]()
    hasOnPRBeenDisruptedOrPaused = false
    hasDisruptionBeenFixed = false

    val requestId = clue(s"Initiate add party async for $partyToReplicate")(
      targetParticipant.parties.add_party_async(
        party = partyToReplicate,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = onPRSetup.topologySerial,
        participantPermission = ParticipantPermission.Submission,
      )
    )

    clue("Wait until OnPR has begun replicating contracts, but has not finished")(
      isReadyForDisruption.future.futureValue
    )

    (requestId, onPRSetup.expectedNumContracts)
  }

  /** Runs a toxiproxy test scenario putting in place the toxic once OnPR has started replicating,
    * and removing the toxic after a while. Also ignores a certain set of log warnings.
    */
  private def runToxiproxyTest(
      scenario: String,
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      invokeToxic: String => Toxic,
  )(implicit env: TestConsoleEnvironment): Unit = clue(s"scenario $scenario") {
    val (alice, bob) = scenariosToParties(scenario)
    val (addPartyRequestId, expectedNumContracts) =
      initiateOnPRAndInitializeTestInterceptor(sourceParticipant, targetParticipant, alice, bob)
    scenariosActuallyRun += scenario

    loggerFactory.assertLogsUnorderedOptional(
      {
        val toxic = clue(s"Put in place toxic $scenario")(invokeToxic(scenario))

        sleepLongEnoughForDisruptionToBeNoticed()
        // Only let the test interceptor know that OnPR has been interrupted after the wait
        // to avoid artificially pausing OnPR activity until after the disruption.
        hasOnPRBeenDisruptedOrPaused = true

        clue(s"Remove toxic $scenario")(toxic.remove())
        hasDisruptionBeenFixed = true

        logger.info("OnPR should be able to finish now that connectivity is reenabled")

        // Wait until both SP and TP report that party replication has completed.
        eventuallyOnPRCompletes(
          sourceParticipant,
          targetParticipant,
          addPartyRequestId,
          expectedNumContracts.toNonNegative,
        )
      },
      // SP and TP-side warning when connection is unexpectedly cancelled, e.g. by GRPC.
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("GrpcSequencerChannelMemberMessageHandler")
        e.warningMessage should include("CANCELLED: client cancelled")
      },
      // Reconnect ping timeout exceeded while connectivity is down.
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("SequencerChannelClientTransport")
        e.warningMessage should include("GrpcClientGaveUp: DEADLINE_EXCEEDED")
      },
      // Ignore warnings related to regular sequencer/client.
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("GrpcConnection")
        e.warningMessage should include("Is the server running?")
      },
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("GrpcConnection")
        e.errorMessage should (include("Request failed for server-") and include(
          "GrpcServerError: UNKNOWN/channel closed"
        ))
      },
      LogEntryOptionality.OptionalMany -> {
        _.shouldBeCantonError(
          LostSequencerSubscription,
          messageAssertion = _ should include("Lost subscription to sequencer"),
          loggerAssertion = _ should include("ResilientSequencerSubscription"),
        )
      },
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("PeriodicAcknowledgements")
        e.warningMessage should include regex "Failed to acknowledge clean timestamp \\(usually because sequencer is down\\)"
      },
      // Transient log noise marking agreement contract done.
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("TransactionProcessor")
        e.warningMessage should include regex "Failed to submit submission due to .*No connection available"
      },
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("PartyReplicationAdminWorkflow")
        e.warningMessage should include regex "Failed to submit submit .*SEQUENCER_REQUEST_FAILED"
      },
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("GrpcSequencerClientTransportPekko")
        e.warningMessage should include("Request failed for sequencer. Is the server running?")
      },
      // GrpcSequencerClientTransportPekko
      // TODO(#26698): Remove UnknownContractSynchronizers warning.
      LogEntryOptionality.OptionalMany -> { e =>
        e.loggerName should include("PartyReplicationAdminWorkflow")
        e.level shouldBe Level.WARN
        e.warningMessage should include regex "UNKNOWN_CONTRACT_SYNCHRONIZERS.*: The synchronizers for the contracts .* are currently unknown due to ongoing contract reassignments or disconnected synchronizers"
      },
    )
  }

  private def timeoutParticipant1Sequencer1(scenario: String, direction: ToxicDirection): Toxic =
    getProxy.underlying.toxics().timeout(scenario, direction, 100)

  "Finish replicating party after source participant upstream network disruption" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      runToxiproxyTest(
        "timeout-sp-upstream",
        sourceParticipant = participant1,
        targetParticipant = participant2,
        invokeToxic = timeoutParticipant1Sequencer1(_, ToxicDirection.UPSTREAM),
      )
  }

  "Finish replicating party after source participant downstream network disruption" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      runToxiproxyTest(
        "timeout-sp-downstream",
        sourceParticipant = participant1,
        targetParticipant = participant2,
        invokeToxic = timeoutParticipant1Sequencer1(_, ToxicDirection.DOWNSTREAM),
      )
  }

  "Finish replicating party after target participant upstream network disruption" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      runToxiproxyTest(
        "timeout-tp-upstream",
        sourceParticipant = participant2,
        targetParticipant = participant1,
        invokeToxic = timeoutParticipant1Sequencer1(_, ToxicDirection.UPSTREAM),
      )
  }

  // TODO(#27175): Unignore this test after figuring out why the regular sequencer client appears to get wedged after
  //  "error however may still be possibly sequenced so we are ignoring the error".
  "Finish replicating party after target participant downstream network disruption" onlyRunWith ProtocolVersion.dev ignore {
    implicit env =>
      import env.*
      runToxiproxyTest(
        "timeout-tp-downstream",
        sourceParticipant = participant2,
        targetParticipant = participant1,
        invokeToxic = timeoutParticipant1Sequencer1(_, ToxicDirection.DOWNSTREAM),
      )
  }

  "Ensure no toxiproxy test scenario skipped" onlyRunWith ProtocolVersion.dev in { _ =>
    val scenariosSkipped = testScenarios.keySet -- scenariosActuallyRun
    scenariosSkipped shouldBe empty
  }
}

// class OnlinePartyReplicationNetworkDisruptionsTestH2
//   extends OnlinePartyReplicationNetworkDisruptionsTest {
//   registerPlugin(new UseH2(loggerFactory))
// }

class OnlinePartyReplicationNetworkDisruptionsTestPostgres
    extends OnlinePartyReplicationNetworkDisruptionsTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
