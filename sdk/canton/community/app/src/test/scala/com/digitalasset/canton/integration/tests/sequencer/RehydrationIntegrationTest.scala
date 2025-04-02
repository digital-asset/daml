// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import cats.syntax.parallel.*
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data
import com.daml.test.evidence.tag.Reliability.ReliabilityTestSuite
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.examples.java.{cycle as C, iou}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, ParticipantInitConfig}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{SequencerConnections, SubmissionRequestAmplification}
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import com.digitalasset.canton.synchronizer.sequencer.config.{
  SequencerNodeConfig,
  SequencerNodeInitConfig,
}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** The operations performed in this test are the following:
  *
  *   1. Setting up a synchronizer with a participant.
  *   1. Generating ledger api data (cycle contracts).
  *   1. Migrating nodes to fresh instances and rehydrating them.
  *   1. Reconnecting the nodes and checking state of the system.
  *
  * In 3, the sequencer is rehydrated from the shared sequencer driver DB and both the mediator and
  * the participant are rehydrated from the sequencer.
  */
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
abstract class RehydrationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with ReliabilityTestSuite {

  private val iterations: Int = 1
  private val transactionLimit: Int = iterations * 3
  private val acsLimit: Int = iterations

  private val staticSynchronizerParameters =
    EnvironmentDefinition.defaultStaticSynchronizerParameters

  private var observedTx: Seq[Transaction] = _
  private var observedAcs: Map[String, CreatedEvent] = _

  private var alice: PartyId = _
  private var synchronizerId: SynchronizerId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition(
      CantonConfig(
        sequencers = Map(
          InstanceName.tryCreate(s"sequencer1") -> SequencerNodeConfig(),
          InstanceName.tryCreate(s"sequencer2") -> SequencerNodeConfig(init =
            SequencerNodeInitConfig(identity = None)
          ),
        ),
        mediators = Map(
          InstanceName.tryCreate(s"mediator1") -> MediatorNodeConfig(),
          InstanceName.tryCreate(s"mediator2") -> MediatorNodeConfig(init =
            InitConfig(identity = None)
          ),
        ),
        participants = Map(
          InstanceName.tryCreate(s"participant1") -> LocalParticipantConfig(),
          InstanceName.tryCreate(s"participant2") -> LocalParticipantConfig(init =
            ParticipantInitConfig(identity = None)
          ),
        ),
        monitoring = MonitoringConfig(
          tracing = TracingConfig(propagation = Propagation.Enabled),
          logging = LoggingConfig(api = ApiLoggingConfig(messagePayloads = true)),
        ),
        features = CantonFeatures(enableRepairCommands = true),
      )
    ).addConfigTransforms(ConfigTransforms.defaultsForNodes*)
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        participant1.start()
        mediator1.start()
        sequencer1.start()
        synchronizerId = env.bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.two,
          staticSynchronizerParameters,
        )

        sequencer1.health.wait_for_initialized()
        mediator1.health.wait_for_initialized()
        sequencer1.health.initialized() shouldBe true
        mediator1.health.initialized() shouldBe true

        participant1.synchronizers.connect_local(
          sequencer1,
          alias = daName,
          // increase this delay in order to avoid the occurrence of SEQUENCER_SUBSCRIPTION_LOST warning
          maxRetryDelayMillis = Some(10L * 60L * 1000L),
        )
        alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant1))
      }

  "generate some ledger data that we want to replay afterwards" in { implicit env =>
    import env.*

    participant1.dars.upload(CantonExamplesPath)

    val runF = (1 to iterations: Seq[Int]).parTraverse_ { i =>
      Future {
        // in each iteration, we create and archive a cycle contract and we create another one so that we build up an ACS
        runCycleRobust(alice, participant1, participant1, s"RUN-CYCLE-#$i", s"run-cycle-$i")
        createCycleContract(participant1, alice, s"LEAKED CYCLE-#$i", s"create-cycle-$i")
      }
    }
    runF.futureValue

    participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new iou.Iou(
        alice.toProtoPrimitive,
        alice.toProtoPrimitive,
        new iou.Amount(100.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq,
    )

    observedTx = participant1.ledger_api.updates
      .flat(
        partyIds = Set(alice),
        completeAfter = transactionLimit + 10,
        timeout = 5.seconds,
      )
      .flatMap {
        case UpdateService.TransactionWrapper(transaction) => Some(transaction)
        case _ => None
      }
    observedAcs = acsAsMap(
      participant1.ledger_api.state.acs
        .of_all(acsLimit + 10)
        .flatMap(_.entry.activeContract.flatMap(_.createdEvent))
    )

    observedTx shouldNot be(empty)
    observedAcs shouldNot be(empty)
  }

  "move all nodes to a fresh database and rehydrate from blockchain" in { implicit env =>
    import env.*

    logger.debug("Start to move everything to fresh nodes")

    val sequencerConnections = SequencerConnections.tryMany(
      Seq(
        sequencer2.sequencerConnection.withAlias(
          SequencerAlias.tryCreate(sequencer2.name)
        )
      ),
      PositiveInt.one,
      SubmissionRequestAmplification.NoAmplification,
    )

    // stop mediator1 and copy it over to the fresh mediator2
    better.files.File.usingTemporaryDirectory("mediator") { dir =>
      val tempDirMediator = dir.pathAsString
      clue("move mediator1 to mediator2") {
        repair.identity.download(mediator1, synchronizerId, tempDirMediator)
        mediator1.stop()

        // Stop sequencer1 and copy it over to the fresh sequencer2, initializing it from beginning.
        //  sequencer2 must reuse the same database as sequencer1.
        better.files.File.usingTemporaryDirectory("sequencer") { dir =>
          val tempDirSequencer = dir.pathAsString
          clue("move sequencer1 to sequencer2") {
            repair.identity.download(sequencer1, synchronizerId, tempDirSequencer)
            participant1
              .stop() // Avoids connection errors due to the sequencer being stopped, will be restarted later for the migration
            sequencer1.stop()
            // architecture-handbook-entry-begin: RehydrationSequencer
            repair.identity.upload(
              sequencer2,
              tempDirSequencer,
              synchronizerId,
              staticSynchronizerParameters,
              sequencerConnections,
            )
            // architecture-handbook-entry-end: RehydrationSequencer
            logger.debug("Moved sequencer1 to sequencer2")
          }
        }

        // architecture-handbook-entry-begin: RehydrationMediator
        repair.identity.upload(
          mediator2,
          tempDirMediator,
          synchronizerId,
          staticSynchronizerParameters,
          sequencerConnections,
        )
        // architecture-handbook-entry-end: RehydrationMediator
        logger.debug("Moved mediator1 to mediator2")
      }
    }

    // Stop participant1 and copy it over to the fresh participant2, which will initialize and stay disconnected.
    better.files.File.usingTemporaryDirectory("participant") { dir =>
      clue("move participant1 to participant2") {
        val tempDirParticipant = dir.pathAsString
        // architecture-handbook-entry-begin: RepairMacroCloneIdentityDownload
        repair.identity.download(participant1, synchronizerId, tempDirParticipant)
        repair.dars.download(participant1, tempDirParticipant)
        participant1.stop()
        // architecture-handbook-entry-end: RepairMacroCloneIdentityDownload

        logger.debug("Moving participant1 to participant2")
        // architecture-handbook-entry-begin: RepairMacroCloneIdentityUpload
        repair.identity.upload(
          participant2,
          tempDirParticipant,
          synchronizerId,
          staticSynchronizerParameters,
          sequencerConnections,
        )
        repair.dars.upload(participant2, tempDirParticipant)
        // architecture-handbook-entry-end: RepairMacroCloneIdentityUpload
        logger.debug("Moved participant1 to participant2")
      }
    }
  }

  protected val expectedLogs: Seq[(LogEntryOptionality, LogEntry => Assertion)] = Seq(
    (
      LogEntryOptionality.OptionalMany,
      _.warningMessage should include("timed out at"),
    )
  )

  "reconnect the participant" in { implicit env =>
    import env.*
    logger.debug("Connecting new participant to new sequencer")

    val sequencerConnections = SequencerConnections.tryMany(
      Seq(
        sequencer2.sequencerConnection.withAlias(
          SequencerAlias.tryCreate(sequencer2.name)
        )
      ),
      PositiveInt.one,
      SubmissionRequestAmplification.NoAmplification,
    )

    loggerFactory.assertLogsUnorderedOptional(
      {
        // architecture-handbook-entry-begin: RepairMacroCloneIdentityConnect
        participant2.synchronizers.connect_by_config(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = sequencerConnections,
            initializeFromTrustedSynchronizer = true,
          )
        )
        // architecture-handbook-entry-end: RepairMacroCloneIdentityConnect

        // send a ping such that we know that the participant has processed everything from the synchronizer
        clue("performing ping") {
          participant2.health.ping(participant2, timeout = 30.seconds)
        }
      },
      expectedLogs *,
    )
  }

  "check that ACS and transaction stream have been reconstructed correctly" in { implicit env =>
    import env.*
    // checking the transaction stream order makes only sense in a single-synchronizer scenario
    // we explicitly do not check the following:
    // * completions because they may differ (e.g., if sequencing the transaction submission times out)
    // * ledger offsets because they are not stable

    val participantToCheck = participant2

    val reconstructedTx = participantToCheck.ledger_api.updates
      .flat(
        partyIds = Set(alice),
        completeAfter = transactionLimit + 10,
        timeout = 5.seconds,
      )
      .flatMap {
        case UpdateService.TransactionWrapper(transaction) => Some(transaction)
        case _ => None
      }
    def stripParticipantSpecificFields(tx: Transaction): Transaction =
      tx
        .copy(offset = 0L, traceContext = None)
        .update(
          _.events := tx.events
            .map(_.event match {
              case Event.Event.Empty => Event.Event.Empty
              case Event.Event.Created(created) =>
                Event.Event.Created(created.copy(offset = 0L, nodeId = 0))
              case Event.Event.Archived(archived) =>
                Event.Event.Archived(archived.copy(offset = 0L, nodeId = 0))
              case Event.Event.Exercised(exercised) =>
                Event.Event.Exercised(exercised.copy(offset = 0L, nodeId = 0))
            })
            .map(Event.apply)
        )
    // We don't care about the ping because the ping doesn't involve alice
    reconstructedTx.size shouldBe observedTx.size
    reconstructedTx.lazyZip(observedTx).foreach { (reconstructed, observed) =>
      withClue(
        s"Transaction at original offset ${observed.offset} and reconstructed offset ${reconstructed.offset}"
      ) {
        stripParticipantSpecificFields(observed) shouldBe
          stripParticipantSpecificFields(reconstructed)
      }
    }

    val reconstructedAcs = acsAsMap(
      participantToCheck.ledger_api.state.acs
        .of_all(acsLimit + 10)
        .map(_.event)
    )

    def stripOffsetAndNodeId(event: CreatedEvent): CreatedEvent =
      event.copy(offset = 0L, nodeId = 0)

    observedAcs.view.mapValues(stripOffsetAndNodeId).toSeq shouldBe
      reconstructedAcs.view.mapValues(stripOffsetAndNodeId).toSeq
  }

  "run a few more operations" in { implicit env =>
    import env.*

    participant2.parties.enable("bob", synchronizeParticipants = Seq(participant2))
    participant2.health.ping(participant2)
  }

  private def acsAsMap(acs: Seq[CreatedEvent]): Map[String, CreatedEvent] =
    acs.map(event => event.contractId -> event).toMap

  private def runCycleRobust(
      party: PartyId,
      initiatorParticipant: ParticipantReference,
      responderParticipant: ParticipantReference,
      id: String,
      commandId: String,
  ): Unit = {
    val cycle = new C.Cycle(id, party.toProtoPrimitive).create.commands.loneElement

    val transaction: data.Transaction =
      initiatorParticipant.ledger_api.javaapi.commands.submit_flat(
        Seq(party),
        Seq(cycle),
        commandId = commandId,
      )
    val Seq(cycleContract) =
      JavaDecodeUtil.decodeAllCreated(C.Cycle.COMPANION)(transaction): @unchecked

    // Wait until the transaction was observed on the responder participant
    val cycleTxId = transaction.getUpdateId
    eventually() {
      responderParticipant.ledger_api.updates.by_id(Set(party), cycleTxId) shouldBe
        Symbol("defined")
    }

    val cycleEx = cycleContract.id.exerciseArchive().commands.asScala.toSeq
    responderParticipant.ledger_api.javaapi.commands.submit_flat(
      Seq(party),
      cycleEx,
      commandId = if (commandId.isEmpty) "" else s"$commandId-response",
    )
  }
}

class ReferenceRehydrationIntegrationTestPostgres extends RehydrationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[Postgres](loggerFactory))
}

// TODO(#16823): Re-enable test. This test requires that the second sequencer reads old blocks from genesis,
//  which requires state transfer and catchup functionality that is not yet available.
class BftOrderingRehydrationIntegrationTestPostgres
//  extends RehydrationIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftOrderingBlockSequencer(loggerFactory))
//}
