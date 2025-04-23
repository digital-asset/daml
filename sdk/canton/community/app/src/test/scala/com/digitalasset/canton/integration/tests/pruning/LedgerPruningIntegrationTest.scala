// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.cycle
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.OffsetOutOfRange
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.UnsafeToPrune
import com.digitalasset.canton.protocol.LfThinContractInst
import com.digitalasset.canton.sequencing.protocol.{Recipients, SubmissionRequest}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{SynchronizerAlias, config}
import org.scalatest.Assertions.fail
import org.scalatest.OptionValues

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters.*

@nowarn("msg=match may not be exhaustive")
abstract class LedgerPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with HasProgrammableSequencer {

  private val reconciliationInterval = JDuration.ofSeconds(1)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  private val internalPruningBatchSize =
    PositiveInt.tryCreate(5) // small enough to exercise batching of prune requests

  // Pick a low max dedup duration so that we don't delay pruning unnecessarily
  private val maxDedupDuration = JDuration.ofSeconds(10)
  private val pruningTimeout =
    Ordering[JDuration].max(
      reconciliationInterval
        .plus(confirmationResponseTimeout.duration)
        .plus(mediatorReactionTimeout.duration),
      maxDedupDuration,
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updatePruningBatchSize(internalPruningBatchSize),
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
      )
      .withSetup { env =>
        import env.*
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
            mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
            reconciliationInterval = config.PositiveDurationSeconds(reconciliationInterval),
          ),
        )
      }

  def pruneAtCurrentLedgerEnd(
      clock: SimClock,
      participant: LocalParticipantReference,
      pingCommand: => Duration,
  ): Unit = {
    val desiredPruningOffsetHex = participant.ledger_api.state.end()

    eventually() {
      clock.advance(pruningTimeout)
      pingCommand
      participant.health.ping(participant.id)
      val safeOffset = participant.pruning
        .find_safe_offset(clock.now.toInstant)
        .getOrElse(throw new IllegalStateException("Can't get safe ts"))
      safeOffset should be > desiredPruningOffsetHex
    }

    participant.pruning.prune(desiredPruningOffsetHex)
  }

  def acsContracts(p: LocalParticipantReference, templateIdO: Option[String] = None)(implicit
      env: TestConsoleEnvironment
  ): Seq[LfThinContractInst] = {
    val all: Seq[LfThinContractInst] =
      p.testing.pcs_search(env.daName, activeSet = true).map(_._2.contractInstance)
    templateIdO match {
      case Some(templateId) =>
        all.filter(_.unversioned.template.qualifiedName.qualifiedName.contains(templateId))
      case None => all
    }
  }

  def acsCount(p: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Int =
    acsContracts(p).size

  def pcsCount(p: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): Int =
    p.testing.pcs_search(env.daName).size

  def fromParticipant(req: SubmissionRequest): Boolean =
    req.sender.code == ParticipantId.Code

  def isCommitment(
      req: SubmissionRequest,
      from: LocalParticipantReference,
      to: LocalParticipantReference,
  ): Boolean =
    // if the participants are not running the `participant.id` calls will fail (inside the sequencer)
    // so first check that the send is from a participant
    fromParticipant(req) &&
      req.sender == from.id &&
      req.batch.envelopes.sizeIs == 1 &&
      req.batch.envelopes.headOption.value.recipients == Recipients.cc(to.id)

  "recover ledger api server after failed prune" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)

    acsCount(participant1) shouldBe 0
    acsCount(participant2) shouldBe 0

    val afterFailedPruneP = Promise[Unit]()

    val sequencer = getProgrammableSequencer(sequencer1.name)

    // Block commitments from participant2 to participant1, to ensure we can trigger a commitment failure
    // Otherwise, existing admin contracts may trigger a commitment if we're unlucky
    sequencer.setPolicy_("Block first commitment from participant2 to participant1") { req =>
      if (
        isCommitment(req, participant2, participant1) ||
        isCommitment(req, participant1, participant2)
      )
        SendDecision.HoldBack(afterFailedPruneP.future)
      else
        SendDecision.Process
    }

    // Move the transactions_end so that we're in the unsafe pruning land
    participant1.health.ping(participant2)

    val Seq(pruneP1At, pruneP2At) =
      Seq(participant1, participant2).map(_.ledger_api.state.end())

    // Pruning should fail as the ACS commitment processor has not processed commitments for the ledger_end yet.
    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant1.pruning.prune(pruneP1At)
      ),
      _.commandFailureMessage should (include(
        s"FAILED_PRECONDITION/${UnsafeToPrune.id}"
      ) or include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/${OffsetOutOfRange.id}\\(9,.*\\): prune_up_to needs to be before ledger end"),
    )

    loggerFactory.assertLogs(
      a[CommandFailure] shouldBe thrownBy(
        participant2.pruning.prune(
          pruneP2At
        )
      ),
      _.commandFailureMessage should (include(
        s"FAILED_PRECONDITION/${UnsafeToPrune.id}"
      ) or include regex
        s"GrpcRequestRefusedByServer: FAILED_PRECONDITION/${OffsetOutOfRange.id}\\(9,.*\\): prune_up_to needs to be before ledger end"),
    )

    sequencer.resetPolicy()
    afterFailedPruneP.success(())

    // Perform a level 2 bong to ensure the ledger api servers are up even after a failed prune
    participant1.testing.bong(
      targets = Set(participant1.id, participant2.id),
      levels = 2,
      timeout = 30.seconds,
    )
    acsCount(participant1) shouldBe 0
    pcsCount(participant1) shouldBe 15

  }

  "correctly prune and restore the system to a functioning state" in { implicit env =>
    import env.*

    def requestJournalSize(
        p: LocalParticipantReference,
        synchronizerAlias: SynchronizerAlias,
        end: Option[CantonTimestamp],
    ): Int = {
      val size = p.testing.state_inspection.requestJournalSize(synchronizerAlias, end = end).value
      size.failOnShutdown
    }

    // Helper function used to snapshot set of transaction ids. As this call takes a long time to complete
    // working with Set of transactions rather than Seq (which can be done by incrementally calling
    // "ledger_completions" with offset-increments, but that would make the test insanely slow (several minutes)
    def collectLedgerApiEvents(
        participant: LocalParticipantReference,
        p1Id: PartyId,
        p2Id: PartyId,
        startAfter: Long = 0L,
    ): Set[String] = {
      val maxEvents = participant.ledger_api.state.end()
      for {
        party <- Set(p1Id, p2Id)
        user <- Set(
          "admin-ping"
        ) // Skipping to speed up test: LedgerApiCommands.userId)
        updateId <- participant.ledger_api.completions
          .list(
            party, // This call is not cheap, so avoiding to call it repeatedly with all userId's for complete ledger picture
            maxEvents.toInt,
            startAfter,
            user,
          )
          .map(_.updateId)
          .filter(_.nonEmpty)
          .toSet[String]
      } yield updateId
    }

    // Helper function to invoke pruning and assert that pruning is observable via ledger api and canton stores.
    def pruneAndCheck(
        participant: LocalParticipantReference,
        ledgerEndBeforeReset: Long,
        pruneAt: Long,
        lastSeenTxTsBeforePruning: CantonTimestamp,
        expectedPcsCount: Int,
        expectedAcsCount: Int,
    ): Unit = {
      logger.debug(s"Pruning ${participant.name}")
      participant.pruning.prune(pruneAt)
      eventually() {
        val ledgerEndAfterPruning =
          participant.ledger_api.state.end()
        ledgerEndBeforeReset should be <= ledgerEndAfterPruning

        // Both active cycle contracts need to have survived
        acsCount(participant) shouldBe expectedAcsCount

        // The only contracts pruned from the pcs should be those of the first bong.
        pcsCount(participant) shouldBe expectedPcsCount

        val msgs = participant.testing.sequencer_messages(daName)
        msgs should not be empty
        forAll(msgs) { msg =>
          msg.timestamp should be >= lastSeenTxTsBeforePruning
        }

        requestJournalSize(participant, daName, end = Some(lastSeenTxTsBeforePruning)) shouldBe 0
      }
    }

    val clock = environment.simClock.value

    val p1Id = participant1.id.adminParty
    val p2Id = participant2.id.adminParty

    val p1UnrelatedPingContracts = pcsCount(participant1)
    val p1UnrelatedActivePingContracts = acsCount(participant1)

    val p2UnrelatedPingContracts = pcsCount(participant2)
    val p2UnrelatedActivePingContracts = acsCount(participant2)

    // Perform a level 2 bong to produce a lot of archived contracts and events to be pruned.
    // Produces 14 contracts.
    participant1.testing.bong(
      targets = Set(participant1.id, participant2.id),
      levels = 2,
      timeout = 30.seconds,
    )

    pcsCount(participant1) shouldBe 14 + p1UnrelatedPingContracts
    // No new ping contracts should remain active
    acsCount(participant1) shouldBe p1UnrelatedActivePingContracts

    // Creates two more contracts (that stay active)
    createCycleContract(
      participant1,
      p1Id,
      "created before pruning, remains active after pruning",
    )
    createCycleContract(
      participant2,
      p2Id,
      "created before pruning, remains active after pruning",
    )

    eventually() {
      acsCount(participant1) shouldBe 1
      acsCount(participant2) shouldBe 1
    }

    // We can't guarantee that all messages prior to the pruning timestamp will be deleted,
    // as some of them might be required for processing of in-flight transactions.
    // However, all messages prior to the timestamp of the last accepted transaction will be pruned.
    // Similarly, the requests before this timestamp will be pruned from the request journal
    val lastSeenTxTsBeforePruning = CantonTimestamp(
      participant1.testing.state_inspection
        .lastSynchronizerOffset(daId)
        .value
        .recordTime
    )

    // Pruning will only delete data prior to successful ACS commitment ticks.
    // We should wait until we're sure to have seen one after the transaction, i.e. for the duration of an
    // AcsCommitment reconciliation interval so that we have non-flaky counts to check after pruning.
    // To avoid slowing tests down massively, use a sim clock and advance the time
    clock.advance(reconciliationInterval.plusSeconds(1))

    val pruningWaitStart = clock.now.toInstant
    val prePrunedPrefix = collectLedgerApiEvents(participant2, p1Id, p2Id)

    val Seq(p1PruningOffset, p2PruningOffset) =
      Seq(participant1, participant2).map(_.ledger_api.state.end())

    // Wait for the mediatorReactionTimeout + confirmationResponseTimeout so that events before pruneBefore are not
    // needed to regenerate ephemeral state upon crash recovery
    // Also wait for the max dedup duration
    val synchronizerParameters =
      sequencer1.topology.synchronizer_parameters.get_dynamic_synchronizer_parameters(daId)

    val decisionTime = pruningWaitStart plus
      synchronizerParameters.confirmationResponseTimeout.asJavaApproximation plus
      synchronizerParameters.mediatorReactionTimeout.asJavaApproximation

    val timeout = JDuration.between(pruningWaitStart, decisionTime)
    clock.advance(Ordering[JDuration].max(timeout, maxDedupDuration))

    createCycleContract(participant1, p1Id, "created after pruning, remains active after pruning")
    createCycleContract(participant2, p2Id, "created after pruning, remains active after pruning")

    eventually() {
      assert(
        participant1.pruning
          .find_safe_offset(clock.now.toInstant)
          .getOrElse(0L)
          >= p1PruningOffset
      )
      assert(
        participant2.pruning
          .find_safe_offset(clock.now.toInstant)
          .getOrElse(0L)
          >= p2PruningOffset
      )
    }

    // Perform a post-prune-cutoff level 2 bong to produce a lot of events that need to survive pruning.
    participant1.testing.bong(
      targets = Set(participant1.id, participant2.id),
      levels = 2,
      timeout = 30.seconds,
    )
    eventually(
      timeUntilSuccess = 60.seconds
    ) {
      pcsCount(participant1) shouldBe 30 + p1UnrelatedPingContracts
      pcsCount(participant2) shouldBe 30 + p2UnrelatedPingContracts
      // Two (Cycle) contracts are still active
      // filter by the template used in this test
      acsContracts(
        p = participant1,
        templateIdO = Some(cycle.Cycle.TEMPLATE_ID.getEntityName),
      ).size shouldBe 2 withClue s"\nactive contracts found: ${acsContracts(p = participant1, templateIdO = Some(cycle.Cycle.TEMPLATE_ID.getEntityName))}"
      acsContracts(
        p = participant2,
        templateIdO = Some(cycle.Cycle.TEMPLATE_ID.getEntityName),
      ).size shouldBe 2 withClue s"\nactive contracts found: ${acsContracts(p = participant2, templateIdO = Some(cycle.Cycle.TEMPLATE_ID.getEntityName))}"
    }

    val prePrunedSuffix = collectLedgerApiEvents(participant2, p1Id, p2Id) -- prePrunedPrefix

    // Make sure that participants' ledger api servers survive a reset and catch up again.
    val p1LedgerEnd = participant1.ledger_api.state.end()
    val p2LedgerEnd = participant2.ledger_api.state.end()
    pruneAndCheck(
      participant1,
      p1LedgerEnd,
      p1PruningOffset,
      lastSeenTxTsBeforePruning,
      16 + p1UnrelatedActivePingContracts,
      2,
    )
    pruneAndCheck(
      participant2,
      p2LedgerEnd,
      p2PruningOffset,
      lastSeenTxTsBeforePruning,
      16 + p2UnrelatedActivePingContracts,
      2,
    )

    // The prefix of the post-prune ledger should match the suffix of the pre-pruned ledger.
    val prunedPrefix =
      collectLedgerApiEvents(
        participant2,
        p1Id,
        p2Id,
        p2PruningOffset, // ledger api only allows reading completions after pruning offset
      )
    prePrunedSuffix shouldBe prunedPrefix

    // check that ledger api continues to work using reset servers by sending bong (concurrent ping)
    participant1.testing.bong(Set(participant2.id, participant1.id))
  }

  "can be pruned twice" in { implicit env =>
    import env.*

    val clock = environment.simClock.value
    pruneAtCurrentLedgerEnd(clock, participant1, participant1.health.ping(participant2))
    pruneAtCurrentLedgerEnd(clock, participant2, participant2.health.ping(participant1))

    val wait = reconciliationInterval
    val waitX2 = wait.multipliedBy(2)

    clock.advance(waitX2)
    participant1.health.ping(participant2)
    clock.advance(wait)

    pruneAtCurrentLedgerEnd(clock, participant1, participant1.health.ping(participant2))

    clock.advance(waitX2)
    participant1.health.ping(participant2)
    clock.advance(waitX2)

    pruneAtCurrentLedgerEnd(clock, participant1, participant1.health.ping(participant2))
  }

  "participants can prune with missing ACS commitments and don't complain when commitments for pruned periods arrive" in {
    implicit env =>
      import env.*
      participants.all.foreach(_.dars.upload(CantonExamplesPath))

      val clock = environment.simClock.value

      val sequencer = getProgrammableSequencer(sequencer1.name)

      val afterPruneP = Promise[Unit]()

      val oneCommitmentHeldBack = new AtomicBoolean(false)

      sequencer.setPolicy_("Block first commitment from participant2 to participant1") { req =>
        if (
          isCommitment(req, participant2, participant1) &&
          oneCommitmentHeldBack.compareAndSet(false, true)
        ) {
          SendDecision.HoldBack(afterPruneP.future)
        } else SendDecision.Process
      }

      val cmd =
        new Iou(
          participant1.adminParty.toProtoPrimitive,
          participant2.adminParty.toProtoPrimitive,
          new Amount(3.50.toBigDecimal, "CHF"),
          List().asJava,
        ).create.commands.asScala.toSeq
      participant1.ledger_api.javaapi.commands.submit_flat(
        Seq(participant1.id.adminParty),
        cmd,
        Some(daId),
      )

      pruneAtCurrentLedgerEnd(clock, participant1, participant1.health.ping(participant2))

      logger.info("Releasing the commitment message")
      afterPruneP.completeWith(Future.unit)
      participant1.health.ping(participant2)

      sequencer.resetPolicy()
  }

  "find_safe_offset returns error when asked to find a safe offset before timestamp without canton ledger state" in {
    implicit env =>
      import env.*
      loggerFactory.assertLogs(
        a[CommandFailure] shouldBe thrownBy(
          participant1.pruning.find_safe_offset(CantonTimestamp.Epoch.minusSeconds(60).toInstant)
        ),
        logEntry => {
          logEntry.commandFailureMessage should include regex
            "GrpcRequestRefusedByServer: FAILED_PRECONDITION/NO_INTERNAL_PARTICIPANT_DATA_BEFORE\\(9,.*\\): No internal participant data to prune up to time"
        },
      )
  }
}

object LedgerPruningIntegrationTest extends OptionValues {

  def acsCount(p: LocalParticipantReference, synchronizerAlias: SynchronizerAlias)(implicit
      tc: TraceContext
  ): Int = {
    val underlying = p.underlying.value
    val size = Await
      .result(
        underlying.sync.stateInspection
          .contractCountInAcs(synchronizerAlias, CantonTimestamp.now()),
        5.seconds,
      )
      .onShutdown(fail("LedgerPruningIntegrationTest"))
      .value
    size
  }
}

//class LedgerPruningIntegrationTestDefault extends LedgerPruningIntegrationTest

class LedgerPruningIntegrationTestPostgres extends LedgerPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

//class LedgerPruningIntegrationTestH2 extends LedgerPruningIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory)
//  )
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}
