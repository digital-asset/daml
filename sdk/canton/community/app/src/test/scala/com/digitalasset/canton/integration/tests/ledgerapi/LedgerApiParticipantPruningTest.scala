// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.ParticipantSynchronizerLimits
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveDurationSeconds}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.damltests.java.test
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.examples.java.paint.OfferToPaintHouseByOwner
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import monocle.macros.syntax.lens.*

import java.time.Duration as JDuration
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

trait LedgerApiParticipantPruningTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  private val transactionTolerance = NonNegativeFiniteDuration.ofSeconds(2)
  private val largeTransactionBatchSize: Int = 100

  private val lowerLedgerApiServerBatchSize: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(
        _.ledgerApi.indexService.activeContractsServiceStreams.maxPayloadsPerPayloadsPage
      )
        .replace(1)
        .focus(_.ledgerApi.indexService.updatesStreams.maxPayloadsPerPayloadsPage)
        .replace(1)
    )

  private val confirmationRequestsMaxRate = NonNegativeInt.tryCreate(2 * largeTransactionBatchSize)

  private def updateFormat(participant: LocalParticipantReference) = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(participant.id.adminParty.toLf -> Filters(Nil)),
            filtersForAnyParty = None,
            verbose = false,
          )
        ),
        transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
      )
    ),
    includeReassignments = None,
    includeTopologyEvents = None,
  )

  // single participant environment to focus on ledger api server pruning rather than acs canton commitments
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(transactionTolerance.asJava),
        lowerLedgerApiServerBatchSize,
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.copy(
            confirmationResponseTimeout = transactionTolerance,
            mediatorReactionTimeout = transactionTolerance,
            reconciliationInterval = PositiveDurationSeconds(transactionTolerance.asJava),
            participantSynchronizerLimits =
              ParticipantSynchronizerLimits(confirmationRequestsMaxRate),
          ),
        )
      }

  "ledger pruning prevents access to pruned transactions and completions" in { implicit env =>
    import env.*
    val beforeLedgerTime = {
      val simClock = environment.simClock.value
      val ts = simClock.now
      simClock.advance(JDuration.ofMillis(1L))
      ts
    }

    participant1.dars.upload(CantonTestsPath)

    // Produce some create and archive events to prune.
    val (prunedUpdateId, _) = createContract(
      participant1
    ) // create a non-archived contract whose event is kept around by the ledger api server
    val Seq(
      _,
      _,
      (offsetInMiddleOfPrunedHistory, _),
      (offsetOfLastPrunedEvent, tsOfLastPrunedEvent),
    ) =
      Seq.range(0, 4).map(_ => createAndExerciseContract(participant1)): @unchecked
    val pruningOffset = offsetOfLastPrunedEvent

    waitUntilSafeToPrune(participant1)(env)
    val (unprunedUpdateId, _) = createContract(
      participant1
    ) // create a non-archived contract whose events is kept around by the ledger api server

    // Before pruning check if looking up offset by timestamp works
    val offsetLookupOfLastPrunedEvent =
      participant1.pruning.get_offset_by_time(tsOfLastPrunedEvent.toInstant)

    // Prune and remember offsets.
    participant1.pruning.prune(pruningOffset)
    val (participant, offsetToPruneUpTo) = (participant1, pruningOffset)
    // user-manual-entry-begin: ManualPruneParticipantNodePrune
    // The prune() method prunes more comprehensively and should be used in most cases.
    participant.pruning.prune(offsetToPruneUpTo)
    // user-manual-entry-end: ManualPruneParticipantNodePrune
    logger.info(s"pruned at $pruningOffset")

    // Subsequent call with lower offset should not error and be no-op and not cause any trouble:
    participant1.pruning.prune(offsetInMiddleOfPrunedHistory)

    // Starting after the last pruned event should be fine:
    participant1.ledger_api.updates
      .transactions(
        partyIds = Set(participant1.id.adminParty),
        completeAfter = 1,
        beginOffsetExclusive = offsetOfLastPrunedEvent,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )
    participant1.ledger_api.completions
      .list(
        participant1.id.adminParty,
        1,
        offsetOfLastPrunedEvent,
      )
    val tx =
      participant1.ledger_api.updates
        .update_by_id(unprunedUpdateId, updateFormat(participant1))
        .value
    tx.updateId shouldBe unprunedUpdateId

    // Starting any earlier should fail:
    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.ledger_api.updates
          .transactions(
            Set(participant1.id.adminParty),
            1,
            offsetInMiddleOfPrunedHistory,
          )
      ),
      _.commandFailureMessage should include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED\\(9,.*\\): Transactions " +
        s"request from ${offsetInMiddleOfPrunedHistory + 1} to .* precedes pruned offset $pruningOffset",
    )

    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.ledger_api.updates
          .transactions(
            partyIds = Set(participant1.id.adminParty),
            completeAfter = 1,
            beginOffsetExclusive = offsetInMiddleOfPrunedHistory,
            transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
      ),
      _.commandFailureMessage should include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED\\(9,.*\\): Transactions " +
        s"request from ${offsetInMiddleOfPrunedHistory + 1} to .* precedes pruned offset $pruningOffset",
    )

    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.ledger_api.completions
          .list(
            participant1.id.adminParty,
            1,
            offsetInMiddleOfPrunedHistory,
          )
      ),
      logEntry => {
        logEntry.commandFailureMessage should include regex
          s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED\\(9,.*\\): Command " +
          s"completions request from ${offsetInMiddleOfPrunedHistory + 1} to .* overlaps with pruned offset $pruningOffset"
      },
    )

    // The pruned transaction should no longer be exposed when querying by transaction id.
    // When we merge pruning upstream, add separate tests for byEvent and flat mode which are not exposed via the canton console
    participant1.ledger_api.updates
      .update_by_id(prunedUpdateId, updateFormat(participant1)) shouldBe None

    // If this turns out to be flaky (because of some background txs), wrap the code below in an eventually.
    val end = participant1.ledger_api.state.end()
    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.pruning.prune(end)
      ),
      logEntry => {
        logEntry.commandFailureMessage should include regex
          "GrpcRequestRefusedByServer: FAILED_PRECONDITION/OFFSET_OUT_OF_RANGE\\(9,.*\\): prune_up_to needs to be before ledger end"
      },
    )

    // Finally check if looking up offset has worked:
    offsetLookupOfLastPrunedEvent.getOrElse(fail()) shouldBe offsetOfLastPrunedEvent

    // Also ensure that looking up a time before ledger begin returns None:
    participant1.pruning.get_offset_by_time(beforeLedgerTime.toInstant) shouldBe None
  }

  "internal participant pruning maintains access to pruned transactions and completions" in {
    implicit env =>
      import env.*

      clue("uploading packages to all nodes") {
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)
      }

      // Produce some create and archive events to prune.
      // create a non-archived contract whose event is kept around by the ledger api server
      val (prunedUpdateId, _) = clue("creating contracts on p2")(createContract(participant2))

      // Create a paint offer
      createAcceptPaintOfferCommand(participant1, participant2)

      // Define the offsets for pruning.
      val Seq(_, _, (offsetInMiddleOfPrunedHistory, _), (offsetOfLastPrunedEvent, _)) =
        Seq.range(0, 4).map(_ => createAndExerciseContract(participant2)): @unchecked
      val pruningOffset = offsetOfLastPrunedEvent

      // Remember the time of the last pre-pruned event for later checking canton's sequenced event store.
      val synchronizerTimestampBeforePruning = CantonTimestamp(
        participant2.testing.state_inspection.lastSynchronizerOffset(daId).value.recordTime
      )

      waitUntilSafeToPrune(participant2)(env)
      val (_unprunedUpdateId, _unprunedCid) = createContract(
        participant2
      ) // create a non-archived contract whose create event is kept around by the ledger api server

      // Prune and remember offsets.
      val (participant, offsetToPruneUpTo) = (participant2, pruningOffset)
      // user-manual-entry-begin: ManualPruneParticipantNodeInternalPrune
      // The prune() method prunes more comprehensively and should be used in most cases.
      participant.pruning.prune_internally(offsetToPruneUpTo)
      // user-manual-entry-end: ManualPruneParticipantNodeInternalPrune
      logger.info(s"pruned internally at $pruningOffset")

      // Pruning internally with lower offset should succeed and be a no-op.
      participant2.pruning.prune_internally(offsetInMiddleOfPrunedHistory)

      // Starting after the last pruned event should be fine:
      participant2.ledger_api.updates
        .transactions(
          partyIds = Set(participant2.id.adminParty),
          completeAfter = 1,
          beginOffsetExclusive = offsetInMiddleOfPrunedHistory,
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        )
      participant2.ledger_api.updates
        .transactions(
          partyIds = Set(participant2.id.adminParty),
          completeAfter = 1,
          beginOffsetExclusive = offsetInMiddleOfPrunedHistory,
        )
      participant2.ledger_api.completions
        .list(
          participant2.id.adminParty,
          1,
          offsetInMiddleOfPrunedHistory,
        )
      val txTree =
        participant2.ledger_api.updates
          .update_by_id(prunedUpdateId, updateFormat(participant2))
          .value
      txTree.updateId shouldBe prunedUpdateId

      // Check that the "internal" sequenced event store has been pruned:
      val lookup = participant2.testing
        .sequencer_messages(daId)
        .toSeq
        .sortBy(_.counter)
      assert(lookup.forall(_.timestamp > synchronizerTimestampBeforePruning))

      // Using eventually in case some admin contract logic still running possibly adding an event between
      // the completions and the prune call.
      val unexplainedSuccess = new AtomicReference[Option[String]](None)
      eventually() {
        val endBeforePrune = participant2.ledger_api.state.end()
        loggerFactory.assertLogs(
          a[CommandFailure] shouldBe thrownBy {
            participant2.pruning.prune_internally(endBeforePrune)

            // should not get here - if we do, check that the end has since moved:
            val endAfterPrune = participant2.ledger_api.state.end(): @unchecked
            if (endBeforePrune >= endAfterPrune)
              unexplainedSuccess.set(
                Some(
                  s"prune_internally has not failed, yet end after prune $endAfterPrune is no larger than end before prune $endBeforePrune"
                )
              )
          },
          logEntry => {
            logEntry.commandFailureMessage should include("FAILED_PRECONDITION/UNSAFE_TO_PRUNE")
          },
        )
      }

      unexplainedSuccess.get shouldBe None

      // Pruning using a negative offset should error:
      val pruneUpTo = -12345678L
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy {
          participant2.pruning.prune_internally(pruneUpTo)
        },
        logEntry => {
          logEntry.commandFailureMessage should include(
            s"Expecting positive value for offset, found $pruneUpTo"
          )
        },
      )

      // Ensure that we can still prune "the normal way":
      participant2.pruning.prune(pruningOffset)

      // And now the ledger-api request should fail:
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy(
          participant2.ledger_api.updates
            .transactions(
              Set(participant2.id.adminParty),
              1,
              offsetInMiddleOfPrunedHistory,
            )
        ),
        _.commandFailureMessage should include regex
          s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED\\(9,.*\\): Transactions " +
          s"request from ${offsetInMiddleOfPrunedHistory + 1} to .* precedes pruned offset $pruningOffset",
      )
  }

  private def waitUntilSafeToPrune(
      participant: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val reconciliationInterval = transactionTolerance
    val clock = environment.simClock.value
    val ledgerEnd = participant.ledger_api.state.end(): @unchecked

    // Produce more create and archive events to have events for ledger api requests to be able to access after pruning.
    Seq.range(0, 4).foreach(_ => createAndExerciseContract(participant))

    // Advance clock long enough to be sure that the last event to be pruned is followed by an acs commitment. Only then
    // invoke pruning.
    eventually(timeUntilSuccess = transactionTolerance.underlying * 10) {
      // Need to add an event after the reconciliation interval to also advance clean head beyond acs commitment tick.
      clock.advance(reconciliationInterval.asJava)
      // ensure participants have observed the new advanced time
      participants.local.foreach(_.testing.fetch_synchronizer_times())

      createAndExerciseContract(participant)

      val timeToPruneUpTo = clock.now
      // user-manual-entry-begin: ManualPruneParticipantNodeSafeOffsetLookup
      val offsetToPruneUpTo = participant.pruning.find_safe_offset(timeToPruneUpTo.toInstant)
      // user-manual-entry-end: ManualPruneParticipantNodeSafeOffsetLookup
      val safeOffset = offsetToPruneUpTo.getOrElse(0L)
      logger.info(s"safe offset $safeOffset compared to $ledgerEnd")
      safeOffset should be > ledgerEnd
    }
  }

  private def createContract(participant: LocalParticipantReference): (String, String) = {
    val partyId = participant.id.adminParty
    val createCmd = new test.Dummy(partyId.toProtoPrimitive).create.commands.asScala.toSeq
    val createTx = participant.ledger_api.javaapi.commands
      .submit(Seq(partyId), createCmd, commandId = s"createContract-${UUID.randomUUID()}")
    val cid = createTx.getEvents.asScala.collectFirst {
      case x if x.toProtoEvent.hasCreated =>
        val contractId = x.toProtoEvent.getCreated.getContractId
        logger.info(s"Created contract $contractId at offset ${createTx.getOffset}")
        contractId
    }.value

    (createTx.getUpdateId, cid)
  }

  private def createAndExerciseContract(
      participant: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment): (Long, CantonTimestamp) = {
    val (_txId, cid) = createContract(participant)

    import env.*
    val partyId = participant.id.adminParty
    val coid =
      participant.ledger_api.javaapi.state.acs
        .await(test.Dummy.COMPANION)(
          partyId,
          (instance: test.Dummy.Contract) => instance.id.contractId == cid,
        )
    logger.info(s"Found contract id ${coid.id.contractId}")
    val clock = environment.simClock.value
    clock.advance(JDuration.ofMillis(1L))

    val exerciseCmd = coid.id.exerciseDummyChoice().commands.asScala.toSeq
    val exerciseTx = participant.ledger_api.javaapi.commands
      .submit(
        Seq(partyId),
        exerciseCmd,
        commandId = s"createAndExerciseContract-${UUID.randomUUID()}",
      )
    exerciseTx.getEvents.asScala.collect {
      case x if x.toProtoEvent.hasArchived =>
        val contractId = x.toProtoEvent.getArchived.getContractId
        logger.info(s"Archived contract $contractId at offset ${exerciseTx.getOffset}")
    }
    val lastEventTimestamp = clock.now
    clock.advance(JDuration.ofMillis(1L))
    (exerciseTx.getOffset, lastEventTimestamp)
  }

  def createAcceptPaintOfferCommand(
      houseOwnerParticipant: LocalParticipantReference,
      painterParticipant: LocalParticipantReference,
  )(implicit env: TestConsoleEnvironment): Command = {
    import env.*

    val uuid = UUID.randomUUID()
    val painter = painterParticipant.adminParty

    // The houseOwner issues an iou
    val houseOwner = houseOwnerParticipant.adminParty
    val createIouCommand =
      new Iou(
        houseOwner.toProtoPrimitive,
        houseOwner.toProtoPrimitive,
        new Amount(1.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq
    val iou = JavaDecodeUtil
      .decodeAllCreated(Iou.COMPANION)(
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(houseOwner),
            createIouCommand,
            commandId = s"participant1-createIou-$uuid",
          )
      )
      .headOption
      .value

    // The houseOwner creates a paint offer
    val createPaintOfferCommand =
      new OfferToPaintHouseByOwner(
        houseOwner.toProtoPrimitive,
        painter.toProtoPrimitive,
        houseOwner.toProtoPrimitive,
        iou.id,
      ).create.commands.asScala.toSeq
    val offer = JavaDecodeUtil
      .decodeAllCreated(OfferToPaintHouseByOwner.COMPANION)(
        houseOwnerParticipant.ledger_api.javaapi.commands.submit(
          Seq(houseOwner),
          createPaintOfferCommand,
          commandId = s"houseOwnerParticipant-createPaintOffer-$uuid",
        )
      )
      .headOption
      .value
    offer.id.exerciseAcceptByPainter().commands.loneElement
  }
}

class LedgerApiParticipantPruningTestDefault extends LedgerApiParticipantPruningTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

//class LedgerApiParticipantPruningTestPostgres extends LedgerApiParticipantPruningTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}
