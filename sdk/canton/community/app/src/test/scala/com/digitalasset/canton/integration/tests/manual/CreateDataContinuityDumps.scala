// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.daml.ledger.api.v2.commands.DisclosedContract
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.PostgresDumpRestore
import com.digitalasset.canton.integration.tests.manual.DataContinuityTest.*
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.version.ProtocolVersion

import java.nio.file.Files

trait CreateBasicDataContinuityDumps extends BasicDataContinuityTestSetup {

  // Copy the generated dumps from docker image to local file system
  override def afterAll(): Unit = {
    DataContinuityTest.synchronizedOperation(
      dumpRestore.copyToLocal(baseDbDumpPath, baseDbDumpPath.path)
    )
    referenceBlockSequencerPlugin.pgPlugin.foreach { plugin =>
      val dumpRestore = PostgresDumpRestore(plugin, forceLocal = forceLocalDumps)
      DataContinuityTest.synchronizedOperation(
        dumpRestore.copyToLocal(baseDbDumpPath, baseDbDumpPath.path)
      )
    }
    super.afterAll()
  }

  def supportedPVS: List[ProtocolVersion]

  "Data continuity with simple contracts" should {
    implicit val folder: FolderName = FolderName("0-simple")

    supportedPVS.foreach { pv =>
      s"correctly generate dumps for (release=$releaseVersion, protocolVersion=$pv)" in { env =>
        withNewProtocolVersion(env, pv) { implicit newEnv =>
          import newEnv.*

          new NetworkBootstrapper(S1M1).bootstrap()
          noDumpFilesOfConfiguredVersionExist(
            Seq[LocalInstanceReference](participant1, participant2, sequencer1, mediator1),
            pv,
          )
          logDebugInformation(logger)

          // run a series of operations for some initial data to act upon
          nodes.local.start()
          participants.all.synchronizers.connect_local(sequencer1, alias = daName)
          participants.all.dars.upload(CantonExamplesPath)
          val alice = participant1.parties.enable(
            "Alice"
          )
          val bob = participant1.parties.enable(
            "Bob"
          )
          participant1.authorizePartyOnParticipant(
            alice,
            participant2,
            daId,
          )

          // Ensure the save directories exist and are owned by the user running this test
          Files.createDirectories(getDumpSaveDirectory(pv).path)
          Files.createDirectories(getDisclosureSaveDirectory(pv).path)

          createCycleContract(participant1, alice, "creation of cycle")
          createTrailingNoneContract(participant1, bob, "creation of trailing none")

          // Dump both the DB and a disclosure for the TrailingNone contract
          dumpStateOfConfiguredVersion(
            Seq(sequencer1),
            Seq(mediator1),
            Seq(participant1, participant2),
            pv,
          )
          val createdEvent = trailingNoneAcsWithBlobs(participant1, bob).head.event
          val disclosure = DisclosedContract(
            templateId = createdEvent.templateId,
            contractId = createdEvent.contractId,
            createdEventBlob = createdEvent.createdEventBlob,
            synchronizerId = "",
          )
          dumpDisclosure(DisclosedContract.toJavaProto(disclosure), pv, "trailing-none")
        }
      }
    }
  }
  "Data continuity with BongScenario from BongTestScenarios" should {
    implicit val folder: FolderName = FolderName("3-bong")

    supportedPVS.foreach { pv =>
      s"correctly generate dumps for release $releaseVersion and protocol version $pv" in { env =>
        withNewProtocolVersion(env, pv) { implicit newEnv =>
          import newEnv.*

          new NetworkBootstrapper(S1M1).bootstrap()
          noDumpFilesOfConfiguredVersionExist(
            Seq[LocalInstanceReference](participant1, participant2, sequencer1, mediator1),
            pv,
          )

          logDebugInformation(logger)
          // run a series of operations for some initial data to act upon
          nodes.local.start()
          participants.all.synchronizers.connect_local(sequencer1, alias = daName)
          val (p1_count, p2_count) = setupBongTest
          dumpStateOfConfiguredVersion(
            Seq(sequencer1),
            Seq(mediator1),
            Seq(participant1, participant2),
            pv,
          )
          runBongTest(p1_count, p2_count, 3)
        }
      }
    }
  }
}

// Extra class because SynchronizerChangeDataContinuityTest use an elaborate set-up with 5 participants and 2 synchronizers
// which isn't needed for the other data continuity tests/would clash with them
trait CreateSynchronizerChangeDataContinuityDumps
    extends SynchronizerChangeDataContinuityTestSetup
    with EntitySyntax {

  // Copy the generated dumps from docker image to local file system
  override def afterAll(): Unit = {
    DataContinuityTest.synchronizedOperation(
      dumpRestore.copyToLocal(baseDbDumpPath, baseDbDumpPath.path)
    )
    referenceBlockSequencerPlugin.pgPlugin.foreach { plugin =>
      val dumpRestore = PostgresDumpRestore(plugin, forceLocal = forceLocalDumps)
      DataContinuityTest.synchronizedOperation(
        dumpRestore.copyToLocal(baseDbDumpPath, baseDbDumpPath.path)
      )
    }
    super.afterAll()
  }

  def supportedPVS: List[ProtocolVersion]

  "Data continuity when reassignment of PaintOffer is started and then continued" should {
    implicit val folder: FolderName = FolderName("4-synchronizer-change")

    supportedPVS.foreach { pv =>
      s"correctly generate and save DB dumps for release $releaseVersion protocol version $pv" in {
        env =>
          withNewProtocolVersion(env, pv) { implicit newEnv =>
            import newEnv.*

            val Alice = "Alice"
            val Bank = "Bank"
            val Painter = "Painter"

            val participants = Seq(P1, P2, P3, P4, P5)
            val sequencers = Seq(sequencer1, sequencer2)
            val mediators = Seq(mediator1, mediator2)

            noDumpFilesOfConfiguredVersionExist(
              mergeLocalInstances(participants, mediators, sequencers),
              pv,
            )

            logger.info("Starting all nodes")

            mergeLocalInstances(participants, mediators, sequencers).foreach(_.start())

            logger.info("Running bootstraps")

            new NetworkBootstrapper(
              NetworkTopologyDescription(
                daName,
                sequencers = Seq(sequencer1),
                mediators = Seq(mediator1),
                synchronizerOwners = Seq(sequencer1),
                synchronizerThreshold = PositiveInt.one,
              ),
              NetworkTopologyDescription(
                acmeName,
                sequencers = Seq(sequencer2),
                mediators = Seq(mediator2),
                synchronizerOwners = Seq(sequencer2),
                synchronizerThreshold = PositiveInt.one,
              ),
            ).bootstrap()

            logger.info("Setting up topology")
            setUp(newEnv)
            setupTopology(Alice, Bank, Painter)
            participants.foreach(_.testing.fetch_synchronizer_times())

            logDebugInformation(logger)
            clue("Creating new DB dumps as no dump files for current version where found") {
              // start reassignment
              val paintOfferUnassignedEvent =
                setupAndUnassign(Alice.toPartyId(), Bank.toPartyId(), Painter.toPartyId())
              dumpStateOfConfiguredVersion(sequencers, mediators, participants, pv)
              // act on state
              clue("starting assignment and paint offer acceptance") {
                assignmentAndPaintOfferAcceptance(
                  Alice.toPartyId(),
                  Bank.toPartyId(),
                  Painter.toPartyId(),
                  paintOfferUnassignedEvent,
                )
              }
            }
          }
      }
    }
  }
}
