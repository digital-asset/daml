// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.instances.list.*
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.commands.Command
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedIncompleteUnassigned
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  DbConfig,
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.TestUtils.hasPersistence
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.DamlPackageLoader
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import monocle.macros.syntax.lens.*
import org.scalactic.source.Position
import org.scalatest.{Assertion, Tag}

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success}

// Implemented:
// - Successful composition of a paint agreement workflow and an Iou workflow to a paint offer accept workflow.
// Possible future extensions:
// - Contract nonexistent
// - Contract already unassigned
// - Contract archived
// - Reassignment initiated by a non-stakeholder
// - stakeholder not on target synchronizer

// Key decisions:
// - submit commands through the ledger API, as this is expected to be more stable than the Read/WriteService.
// - after reassignments, check presence/absence of contracts on all synchronizers

object SynchronizerChangeIntegrationTest {
  final case class Config(
      simClock: Boolean,
      assignmentExclusivityTimeout: NonNegativeFiniteDuration,
  )
}

/** Abstract class for tests of the synchronizer change demo scenario.
  */
abstract class SynchronizerChangeIntegrationTest(config: SynchronizerChangeIntegrationTest.Config)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with AcsInspection
    with HasProgrammableSequencer {

  protected val partyCounter = new AtomicInteger(1)

  lazy val darPaths: Seq[String] = Seq(CantonExamplesPath)
  lazy val Packages: Map[PackageId, Ast.Package] = DamlPackageLoader
    .getPackagesFromDarFiles(darPaths)
    .value

  protected lazy val simClockTransform: ConfigTransform =
    if (config.simClock) ConfigTransforms.useStaticTime
    else ConfigTransforms.identity

  protected lazy val exclusivityTimeout: NonNegativeFiniteDuration =
    config.assignmentExclusivityTimeout

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5_S1M1_S1M1
      .addConfigTransforms(
        simClockTransform, // required such that late message processing warning isn't emitted
        _.focus(_.monitoring.logging.delayLoggingThreshold)
          .replace(NonNegativeFiniteDurationConfig.ofDays(100)),
      )
      .addConfigTransforms(additionalConfigTransforms*)
      .withSetup(setUp)

  protected def additionalConfigTransforms: Seq[ConfigTransform] = Seq.empty

  protected def setupTopology(alice: String, bank: String, painter: String)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    logger.debug(s"Allocating parties $alice, $bank and $painter")
    PartiesAllocator(participants.all.toSet)(
      newParties = Seq(alice -> P5, bank -> P1, painter -> P4),
      targetTopology = Map(
        alice -> Map(
          paintSynchronizerId -> (PositiveInt.one, Set(P5.id -> ParticipantPermission.Submission)),
          iouSynchronizerId -> (PositiveInt.one, Set(
            P5.id -> ParticipantPermission.Submission,
            P2.id -> ParticipantPermission.Submission,
          )),
        ),
        painter -> Map(
          paintSynchronizerId -> (PositiveInt.one, Set(P4.id -> ParticipantPermission.Submission)),
          iouSynchronizerId -> (PositiveInt.one, Set(
            P4.id -> ParticipantPermission.Submission,
            P3.id -> ParticipantPermission.Submission,
          )),
        ),
        bank -> Map(
          iouSynchronizerId -> (PositiveInt.one, Set(P1.id -> ParticipantPermission.Submission))
        ),
      ),
    )

    darPaths.foreach { darPath =>
      participants.all.foreach(p => p.dars.upload(darPath))
    }

    // Advance the simClock to trigger time-proof requests, if present
    val clock = environment.simClock
    clock.foreach(_.advance(java.time.Duration.ofSeconds(5)))

    val partyAssignment: Set[(PartyId, ParticipantReference)] =
      Set(alice -> P5, alice -> P2, painter -> P4, painter -> P3, bank -> P1).map {
        case (party, participant) => (party.toPartyId(), participant)
      }

    logger.debug(s"Waiting to see topology assignments for $alice, $bank and $painter")
    List(P1, P2, P3, P4, P5).foreach { x =>
      x.parties.await_topology_observed(partyAssignment)
    }
  }

  protected def withUniqueParties[T](
      test: (PartyId, PartyId, PartyId) => T
  )(implicit env: TestConsoleEnvironment): T = {
    val pc = partyCounter.getAndIncrement()
    val Alice = s"Alice$pc"
    val Bank = s"Bank$pc"
    val Painter = s"Painter$pc"

    setupTopology(Alice, Bank, Painter)

    test(Alice.toPartyId(), Bank.toPartyId(), Painter.toPartyId())
  }

  protected def findPackageIdOf(
      moduleName: String
  )(implicit env: TestConsoleEnvironment): PackageId =
    // It doesn't matter which participant we use as they all have the same package information
    findPackageIdOf(moduleName, P1)

  // These definition have been kept around to minimize the diff when porting to Daml 3
  def iouSynchronizerAlias(implicit env: FixtureParam): SynchronizerAlias = env.daName
  def paintSynchronizerAlias(implicit env: FixtureParam): SynchronizerAlias = env.acmeName

  def P1(implicit env: FixtureParam): LocalParticipantReference = env.participant1
  def P2(implicit env: FixtureParam): LocalParticipantReference = env.participant2
  def P3(implicit env: FixtureParam): LocalParticipantReference = env.participant3
  def P4(implicit env: FixtureParam): LocalParticipantReference = env.participant4
  def P5(implicit env: FixtureParam): LocalParticipantReference = env.participant5

  def iouSynchronizerId(implicit env: FixtureParam): SynchronizerId = env.daId
  def paintSynchronizerId(implicit env: FixtureParam): SynchronizerId = env.acmeId

  val PaintModule: String = "Paint"
  val IouModule: String = "Iou"

  val IouTemplate = "Iou"
  val DiscloseIouTemplate = "DiscloseIou"
  val GetCashTemplate = "GetCash"

  val PaintHouseTemplate = "PaintHouse"
  val OfferToPaintHouseByOwnerTemplate = "OfferToPaintHouseByOwner"

  protected def createIou(alice: PartyId, bank: PartyId, painter: PartyId)(implicit
      env: TestConsoleEnvironment
  ): Iou.Contract = {
    logger.info("Bank creates Alice's IOU")

    val price = new Amount(100.toBigDecimal, "USD")

    // iouId <- submit bank do
    //   create $ Iou with payer = bank; owner = alice; amount = price; viewers = []
    val createIouCmd = new Iou(
      bank.toProtoPrimitive,
      alice.toProtoPrimitive,
      price,
      List.empty.asJava,
    ).create.commands.asScala.toSeq
    val iou = clue("create-iou") {
      JavaDecodeUtil
        .decodeAllCreated(Iou.COMPANION)(
          P1.ledger_api.javaapi.commands
            .submit_flat(Seq(bank), createIouCmd)
        )
        .loneElement
    }
    val iouId = iou.id.toLf

    assertInLedgerAcsSync(Seq(P1), bank, iouSynchronizerId, iouId)
    assertInLedgerAcsSync(Seq(P2, P5), alice, iouSynchronizerId, iouId)
    assertNotInLedgerAcsSync(Seq(P3, P4), painter, iouSynchronizerId, iouId)

    iou
  }

  protected def createPaintOfferCmd(
      alice: PartyId,
      bank: PartyId,
      painter: PartyId,
      iouId: LfContractId,
  )(implicit env: TestConsoleEnvironment): Command = {
    import env.*
    logger.info("Alice creates the paint offer")

    // create $ OfferToPaintHouseByOwner with painter = painter; houseOwner = alice; bank = bank; iouId = iouId
    ledger_api_utils.create(
      findPackageIdOf(PaintModule),
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
      Map[String, Any](
        "painter" -> painter,
        "houseOwner" -> alice,
        "bank" -> bank,
        "iouId" -> iouId,
      ),
    )
  }

  protected def createPaintOffer(
      alice: PartyId,
      bank: PartyId,
      painter: PartyId,
      submittingParticipant: LocalParticipantReference,
      allParticipants: Seq[LocalParticipantReference],
  )(implicit
      env: TestConsoleEnvironment
  ): LfContractId = {
    import env.*

    val iou = createIou(alice, bank, painter)
    val iouId = iou.id.toLf
    val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
    submittingParticipant.ledger_api.commands.submit(
      Seq(alice),
      Seq(cmd),
      Some(paintSynchronizerId),
    )

    searchAcsSync(
      allParticipants,
      paintSynchronizerAlias.unwrap,
      PaintModule,
      OfferToPaintHouseByOwnerTemplate,
    )
  }

  protected def getIncompleteUnassignedContracts(
      participantRefs: Seq[LocalParticipantReference],
      party: PartyId,
  ): WrappedIncompleteUnassigned =
    participantRefs
      .map { participantRef =>
        participantRef.ledger_api.state.acs
          .incomplete_unassigned_of_party(party)
          .map(wrappedUnassigned =>
            WrappedIncompleteUnassigned(comparableIncompleteUnassigned(wrappedUnassigned.entry))
          )
      }
      .toSet
      .loneElement // Stores should all have the same contents
      .loneElement // There should only be one reassignment

  protected def setUp(testEnvironment: TestConsoleEnvironment): Unit = {
    implicit val env: TestConsoleEnvironment = testEnvironment
    import env.*

    sequencer1.topology.synchronizer_parameters
      .propose_update(
        iouSynchronizerId,
        _.update(assignmentExclusivityTimeout = exclusivityTimeout.toConfig),
      )
    sequencer2.topology.synchronizer_parameters
      .propose_update(
        paintSynchronizerId,
        _.update(assignmentExclusivityTimeout = exclusivityTimeout.toConfig),
      )

    Seq(P4, P5).foreach(
      _.synchronizers.connect_local(sequencer2, alias = paintSynchronizerAlias.unwrap)
    )
    Seq(P1, P2, P3, P4, P5).foreach(
      _.synchronizers.connect_local(sequencer1, alias = iouSynchronizerAlias.unwrap)
    )
  }
}

abstract class SynchronizerChangeSimClockIntegrationTest
    extends SynchronizerChangeIntegrationTest(
      SynchronizerChangeIntegrationTest.Config(
        simClock = true,
        assignmentExclusivityTimeout = NonNegativeFiniteDuration.tryOfMinutes(10L),
      )
    )
    with SecurityTestSuite {

  override protected def additionalConfigTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.parameters.reassignmentTimeProofFreshnessProportion).replace(NonNegativeInt.zero)
    )
  )

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  "The paint fence scenario" when {

    "assignment triggered automatically after the exclusivity timeout" must {
      "succeed" taggedAs SecurityTest(
        SecurityTest.Property.Integrity,
        "virtual shared ledger",
        Attack(
          "a malicious participant",
          "initiates an unassignment without a corresponding assignment",
          "an honest participant initiates the assignment",
        ),
      ) in { implicit env =>
        withUniqueParties { case (alice, bank, painter) =>
          val participants = Seq(P4, P5)
          val paintOfferId = createPaintOffer(alice, bank, painter, P5, participants)

          logger.info("Painter reassigns the paint offer to the IOU synchronizer.")

          val reassignmentId =
            P4.ledger_api.commands.submit_unassign(
              painter,
              paintOfferId,
              paintSynchronizerId,
              iouSynchronizerId,
            )

          val clock = env.environment.simClock.value

          logger.info(
            s"An assignment for $reassignmentId is triggered automatically after the exclusivity timeout"
          )

          val margin = NonNegativeFiniteDuration.tryOfSeconds(1)

          // Advance clock just before the exclusivity timeout
          clock.advance(exclusivityTimeout.unwrap.minus(margin.unwrap))
          participants.foreach(_.testing.fetch_synchronizer_times())
          checkIncompleteUnassignedContracts(
            participants,
            painter,
          ) // assignment did not happen yet

          // Get reassignment from the store
          val unassignedEvent = getIncompleteUnassignedContracts(participants, painter)

          val targetTimestamp = CantonTimestamp
            .fromProtoTimestamp(unassignedEvent.assignmentExclusivity.value)
            .value + exclusivityTimeout
          // Advance clock to the exclusivity timeout so that the automatic assignment can be triggered
          clock.advanceTo(targetTimestamp)
          participants.foreach(_.testing.fetch_synchronizer_times())

          // The reassignment store should be empty once the automatic assignment has completed
          checkReassignmentStoreIsEmpty(iouSynchronizerId, participants, painter)
        }

      }

      "succeed over shutdowns" in { implicit env =>
        import env.*

        // Disable this test if we're not running with persistence, as shutdowns aren't meaningful in this context
        // Don't use `assume` here as the TestFailedException causes the log checking to fail
        if (hasPersistence(P4.config.storage)) {

          withUniqueParties { case (alice, bank, painter) =>
            P4.health.ping(P5)

            val participants = Seq(P4, P5)

            val clock = environment.simClock.value
            clock.advance(NonNegativeFiniteDuration.tryOfSeconds(1).unwrap)
            participants.foreach(_.testing.fetch_synchronizer_times())

            val paintOfferId = createPaintOffer(alice, bank, painter, P5, participants)

            logger.info("Painter reassigns the paint offer to the IOU synchronizer.")

            val reassignmentId =
              P4.ledger_api.commands.submit_unassign(
                painter,
                paintOfferId,
                paintSynchronizerId,
                iouSynchronizerId,
              )

            logger.info(s"Reassigning participants stop")
            P4.stop()
            P5.stop()

            logger.info(s"Reassigning participants start")
            P4.start()
            P5.start()

            for {
              p <- Seq(P4, P5)
              d <- Seq(iouSynchronizerAlias, paintSynchronizerAlias)
            } yield {
              p.synchronizers.reconnect(d)
            }

            // Perform pings, to ensure that the participants are fully reconnected before the clock is advanced
            // Otherwise, the reconnection logic can log a WARN
            P4.health.ping(P5, synchronizerId = Some(iouSynchronizerId))
            P4.health.ping(P5, synchronizerId = Some(paintSynchronizerId))

            logger.info(
              s"An assignment for $reassignmentId is triggered automatically after the exclusivity timeout"
            )

            val margin = NonNegativeFiniteDuration.tryOfSeconds(1)

            // Advance clock just before the exclusivity timeout
            clock.advance(exclusivityTimeout.unwrap.minus(margin.unwrap))
            participants.foreach(_.testing.fetch_synchronizer_times())
            checkIncompleteUnassignedContracts(
              participants,
              painter,
            ) // assignment did not happen yet

            // Get reassignment from the store
            val unassignedEvent = getIncompleteUnassignedContracts(participants, painter)
            val targetTimestamp = CantonTimestamp
              .fromProtoTimestamp(
                unassignedEvent.assignmentExclusivity.value
              )
              .value + exclusivityTimeout
            // Advance clock to the exclusivity timeout so that the automatic assignment can be triggered
            clock.advanceTo(targetTimestamp)
            participants.foreach(_.testing.fetch_synchronizer_times())

            // The reassignment store should be empty once the automatic assignment has completed
            checkReassignmentStoreIsEmpty(iouSynchronizerId, participants, painter)
          }
        }
      }

      "take parameters changes into account" in { implicit env =>
        import env.*

        withUniqueParties { case (alice, bank, painter) =>
          val participants = Seq(P4, P5)

          val clock = env.environment.simClock.value
          clock.advance(NonNegativeFiniteDuration.tryOfSeconds(1).unwrap)
          participants.foreach(_.testing.fetch_synchronizer_times())

          val exclusivityTimeoutIncrease = NonNegativeFiniteDuration.tryOfSeconds(2)
          val updatedExclusivityTimeout = exclusivityTimeout + exclusivityTimeoutIncrease

          // Ensure initial assignmentExclusivityTimeout is the expected one
          sequencer1.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(
              daId
            )
            .assignmentExclusivityTimeout shouldBe exclusivityTimeout.toConfig

          // Increase assignmentExclusivityTimeout by exclusivityTimeoutIncrease
          sequencer1.topology.synchronizer_parameters.propose_update(
            iouSynchronizerId,
            _.update(assignmentExclusivityTimeout = updatedExclusivityTimeout.toConfig),
          )

          sequencer1.topology.synchronizer_parameters
            .get_dynamic_synchronizer_parameters(daId)
            .assignmentExclusivityTimeout shouldBe updatedExclusivityTimeout.toConfig

          eventually() {
            P4.topology.synchronizer_parameters
              .get_dynamic_synchronizer_parameters(synchronizerId = iouSynchronizerId)
              .assignmentExclusivityTimeout shouldBe updatedExclusivityTimeout.toConfig
          }

          /*
            Advance time a bit

            We need that otherwise, the timestamp that determines the synchronizer parameters
            (via a time proof) is before the change of assignmentExclusivityTimeout.
           */
          val baseTime = clock.now
          val newTime = baseTime + NonNegativeFiniteDuration.tryOfMillis(1)
          clock.advanceTo(newTime)
          // any event still pending from the sequencer on P4 that gets consumed between
          // the clock.advance above and the fetch time below will make the
          // test hang (as we'll schedule a fetch synchronizer time in the future, but
          // that future will never appear)
          participants.foreach(_.testing.fetch_synchronizer_times())
          P4.testing.await_synchronizer_time(iouSynchronizerAlias, newTime, 5.seconds)

          // Trigger flow
          val paintOfferId = createPaintOffer(alice, bank, painter, P5, participants)
          logger.info("Painter reassigns the paint offer to the IOU synchronizer.")

          val reassignmentId =
            P4.ledger_api.commands.submit_unassign(
              painter,
              paintOfferId,
              paintSynchronizerId,
              iouSynchronizerId,
            )

          // Advance clock at the initial exclusivity timeout
          clock.advance(exclusivityTimeout.unwrap)
          participants.foreach(_.testing.fetch_synchronizer_times())
          checkIncompleteUnassignedContracts(
            participants,
            painter,
          ) // assignment did not happen yet

          // Get reassignment from the store
          val unassignedEvent = getIncompleteUnassignedContracts(participants, painter)
          val targetTimestamp = CantonTimestamp
            .fromProtoTimestamp(
              unassignedEvent.assignmentExclusivity.value
            )
            .value + updatedExclusivityTimeout
          // Advance clock at the updated exclusivity timeout
          clock.advanceTo(targetTimestamp)
          logger.info(
            s"An assignment for $reassignmentId is triggered automatically after the exclusivity timeout"
          )
          participants.foreach(_.testing.fetch_synchronizer_times())
          checkReassignmentStoreIsEmpty(
            iouSynchronizerId,
            participants,
            painter,
          ) // assignment should be done
        }

      }

    }

    "errors do not occur when an assignment is triggered manually" must {
      "succeed" in { implicit env =>
        import env.*

        withUniqueParties { case (alice, bank, painter) =>
          val participants = Seq(P4, P5)

          val clock = env.environment.simClock.value
          clock.advance(NonNegativeFiniteDuration.tryOfSeconds(1).unwrap)
          participants.foreach(_.testing.fetch_synchronizer_times())

          val paintOfferId = createPaintOffer(alice, bank, painter, P5, participants)

          logger.info("Painter reassigns the paint offer to the IOU synchronizer.")

          // paintOffer is in the PaintSynchronizer
          assertInAcsSync(participants, paintSynchronizerAlias, paintOfferId)

          // unassign from PaintSynchronizer to IouSynchronizer
          val unassignedEvent =
            P4.ledger_api.commands
              .submit_unassign(
                painter,
                paintOfferId,
                paintSynchronizerId,
                iouSynchronizerId,
              )
              .unassignedEvent

          P4.ledger_api.commands.submit_assign(
            painter,
            unassignedEvent.unassignId,
            paintSynchronizerId,
            iouSynchronizerId,
          )

          // paintOffer is assigned to IouSynchronizer (assignment has completed)
          assertInAcsSync(Seq(P4, P5), iouSynchronizerAlias, paintOfferId)

          // Trigger the automatic assignment after a assignment has already been completed
          val automaticAssignmentTime = exclusivityTimeout.unwrap
          val baseTime = clock.now
          clock.advance(automaticAssignmentTime)
          participants.foreach(_.testing.fetch_synchronizer_times())

          // paintOffer is still in the IouSynchronizer
          assertInAcsSync(Seq(P4, P5), iouSynchronizerAlias, paintOfferId)

          // Wait for the PaintSynchronizer to reach a time exceeding the exclusivity timeout P4
          P4.testing.await_synchronizer_time(
            paintSynchronizerAlias,
            baseTime + exclusivityTimeout,
            5.seconds,
          )

          // TODO(i9502): Work out why this workaround is required, and remove it if possible
          clock.advance(JDuration.ofSeconds(1))
          participants.foreach(_.testing.fetch_synchronizer_times())

          // Check that reassignments can still be completed
          P4.ledger_api.commands
            .submit_reassign(
              painter,
              paintOfferId,
              iouSynchronizerId,
              paintSynchronizerId,
            )

          // paintOffer is in the PaintSynchronizer
          assertInAcsSync(Seq(P4, P5), paintSynchronizerAlias, paintOfferId)
        }

      }
    }
  }
}

class SynchronizerChangeSimClockIntegrationTestPostgres
    extends SynchronizerChangeSimClockIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

trait SynchronizerChangeRealClockIntegrationTest
    extends AbstractSynchronizerChangeRealClockIntegrationTest {

  "The paint fence scenario" when {
    "executed on the Iou synchronizer" must {
      "succeed" in { implicit env =>
        withUniqueParties { case (alice, bank, painter) =>
          val iou = createIou(alice, bank, painter)
          val iouId = iou.id.toLf

          // paintOfferId <- submit alice do
          //   create $ OfferToPaintHouseByOwner with painter = painter; houseOwner = alice; bank = bank; iouId = iouId
          val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
          clue("creating paint offer") {
            P2.ledger_api.commands.submit(Seq(alice), Seq(cmd))
          }
          val paintOfferId =
            searchAcsSync(
              Seq(P2, P3, P4, P5),
              iouSynchronizerAlias,
              PaintModule,
              OfferToPaintHouseByOwnerTemplate,
            )
          assertInLedgerAcsSync(Seq(P2, P5), alice, iouSynchronizerId, paintOfferId)
          assertInLedgerAcsSync(Seq(P3, P4), painter, iouSynchronizerId, paintOfferId)

          assertSizeOfAcs(
            Seq(P2, P3, P4, P5),
            iouSynchronizerAlias,
            s"^$PaintModule",
            1,
          )
          assertSizeOfAcs(
            Seq(P4, P5),
            paintSynchronizerAlias,
            s"^$PaintModule",
            0,
          )

          val (painterIouId, _) = acceptPaintOffer(alice, bank, painter, paintOfferId)

          callIou(alice, bank, painter, painterIouId)
        }
      }
    }

    "executed on the Iou and Paint synchronizer" must {
      "succeed" in { implicit env: TestConsoleEnvironment =>
        withUniqueParties { case (alice, bank, painter) =>
          val paintOfferUnassignedEvent = setupAndUnassign(alice, bank, painter)
          assignmentAndPaintOfferAcceptance(
            alice,
            bank,
            painter,
            paintOfferUnassignedEvent,
          )
        }
      }

      "in-flight reassignments searchable over GRPC" in { implicit env: TestConsoleEnvironment =>
        import env.*

        withUniqueParties { case (alice, bank, painter) =>
          val iou = createIou(alice, bank, painter)
          val iouId = iou.id.toLf

          // paintOfferId <- submit alice do
          //   create $ OfferToPaintHouseByOwner with painter = painter; houseOwner = alice; bank = bank; iouId = iouId
          val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
          P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))
          val paintOfferId =
            searchAcsSync(
              Seq(P4, P5),
              paintSynchronizerAlias,
              PaintModule,
              OfferToPaintHouseByOwnerTemplate,
            )

          // Fetch the target synchronizer time
          val targetSynchronizerTime =
            P4.testing.fetch_synchronizer_time(iouSynchronizerAlias, 2.seconds)

          logger.info("Painter reassignments the paint offer to the IOU synchronizer.")
          // Unassign paint offer
          P4.ledger_api.commands.submit_unassign(
            painter,
            paintOfferId,
            paintSynchronizerId,
            iouSynchronizerId,
          )
          val unassignedEvents =
            P4.ledger_api.state.acs.incomplete_unassigned_of_party(painter)

          val unassignedEvent = unassignedEvents.loneElement

          // The time proof can vary depending on other concurrent events
          (CantonTimestamp
            .fromProtoTimestamp(unassignedEvent.assignmentExclusivity.value)
            .value - targetSynchronizerTime).toMillis should be <= 5000L
        }
      }

      "contract synchronizers can be looked-up over grpc" in { implicit env =>
        withUniqueParties { case (alice, bank, painter) =>
          val iou = createIou(alice, bank, painter)
          val iouId = iou.id.toLf

          val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
          // same as above
          P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))
          val paintOfferId =
            searchAcsSync(
              Seq(P4, P5),
              paintSynchronizerAlias,
              PaintModule,
              OfferToPaintHouseByOwnerTemplate,
            )
          eventually() {
            forAll(List(P4, P5)) { p =>
              val synchronizerIdOfContract = p.ledger_api.state.acs
                .active_contracts_of_party(painter)
                .map(c => c.synchronizerId -> c.createdEvent)
                .find { case (_, createdEventO) =>
                  createdEventO.exists(e => e.contractId == paintOfferId.toProtoPrimitive)
                }
                .map(_._1)
              synchronizerIdOfContract shouldBe Some(paintSynchronizerId.toProtoPrimitive)

            }
          }

          P4.ledger_api.commands
            .submit_reassign(
              painter,
              paintOfferId,
              paintSynchronizerId,
              iouSynchronizerId,
            )

          eventually() {
            forAll(List(P2, P3, P4, P5)) { p =>
              val synchronizerId = p.ledger_api.state.acs
                .active_contracts_of_party(painter)
                .map(c => c.synchronizerId -> c.createdEvent)
                .find { case (_, createdEventO) =>
                  createdEventO.exists(e => e.contractId == paintOfferId.toProtoPrimitive)
                }
                .map(_._1)

              synchronizerId.value shouldBe iouSynchronizerId.toProtoPrimitive
            }
          }
        }
      }
    }

  }

  "Racy assignments" must {
    "be detected" in { implicit env: TestConsoleEnvironment =>
      import env.*

      withUniqueParties { case (alice, bank, painter) =>
        val iou = createIou(alice, bank, painter)
        val iouId = iou.id.toLf

        val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
        P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))
        val paintOfferId =
          searchAcsSync(
            Seq(P4, P5),
            paintSynchronizerAlias,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
          )

        // Disconnect P2 and P3 such that the ledger API server does not complain about repeatedly "creating" the contract
        P2.synchronizers.disconnect(iouSynchronizerAlias)
        P3.synchronizers.disconnect(iouSynchronizerAlias)

        def racyAssignment(
            sourceAlias: SynchronizerAlias,
            targetAlias: SynchronizerAlias,
        ): Unit = {
          val sourceId = getInitializedSynchronizer(sourceAlias).synchronizerId
          val targetId = getInitializedSynchronizer(targetAlias).synchronizerId

          logger.info("Paint offer is unassigned")
          val paintOfferUnassignedEvent =
            P4.ledger_api.commands
              .submit_unassign(
                painter,
                paintOfferId,
                sourceId,
                targetId,
              )
              .unassignedEvent

          def assign(participant: ParticipantReference, party: PartyId): Unit =
            participant.ledger_api.commands.submit_assign(
              party,
              paintOfferUnassignedEvent.unassignId,
              sourceId,
              targetId,
              timeout = 10 seconds,
            )

          // Wait until P5 sees the unassignment result so that assignments do not fail with `UnassignmentIncomplete`
          eventually() {
            P5.ledger_api.state.acs
              .incomplete_unassigned_of_party(painter)
              .map(_.unassignId) should contain(paintOfferUnassignedEvent.unassignId)
          }

          logger.info(s"Racy assignments of $paintOfferUnassignedEvent occur")
          val assignmentsF = List(
            Future(assign(P4, painter)),
            Future(assign(P5, alice)),
            Future(assign(P4, painter)),
            Future(assign(P5, alice)),
          ).parTraverse(_.transform {
            case Success(_) => Success(1)
            case Failure(_) =>
              Success(0)
          })

          val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(2))
          val successfulAssignments = assignmentsF.futureValue(patience, Position.here).sum

          withClue("Number of successful assignments") {
            successfulAssignments shouldBe 1
          }

          searchAcsSync(
            Seq(P4),
            targetAlias,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
            stakeholder = Some(painter),
          ) shouldBe paintOfferId
          searchAcsSync(
            Seq(P5),
            targetAlias,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
            stakeholder = Some(alice),
          ) shouldBe paintOfferId

          assertNotInAcsSync(
            Seq(P4),
            sourceAlias,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
            stakeholder = Some(painter),
          )
          assertNotInAcsSync(
            Seq(P5),
            sourceAlias,
            PaintModule,
            OfferToPaintHouseByOwnerTemplate,
            stakeholder = Some(alice),
          )

          def assertAssignmentFails(
              participant: ParticipantReference,
              party: PartyId,
          ): Assertion =
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              assign(participant, party),
              _.commandFailureMessage should
                (include("Request failed for participant") and
                  include("\n  GrpcClientError: INVALID_ARGUMENT/INVALID_ARGUMENT") and
                  include("reassignment already completed")),
            )

          assertAssignmentFails(P5, alice)
          assertAssignmentFails(P4, painter)
        }

        val synchronizers = Seq(paintSynchronizerAlias, iouSynchronizerAlias)
        for (i <- 0 to 5) {
          val sourceSynchronizer = synchronizers(i % 2)
          val targetSynchronizer = synchronizers((i + 1) % 2)
          racyAssignment(sourceSynchronizer, targetSynchronizer)
        }

        // Test that the participants are still alive
        assertPingSucceeds(P4, P5, synchronizerId = Some(paintSynchronizerId))
        assertPingSucceeds(P5, P4, synchronizerId = Some(iouSynchronizerId))

        // Reconnect P2 and P3 to preserve state
        P2.synchronizers.reconnect(iouSynchronizerAlias, synchronize = Some(timeouts.default))
        P3.synchronizers.reconnect(iouSynchronizerAlias, synchronize = Some(timeouts.default))
      }

    }
  }

  "unassignment event arrives after the assignment for the reassigning participant" in {
    implicit env =>
      import env.*

      withUniqueParties { case (alice, bank, painter) =>
        val iou = createIou(alice, bank, painter)
        val iouId = iou.id.toLf

        val cmd = createPaintOfferCmd(alice, bank, painter, iouId)
        P5.ledger_api.commands.submit(Seq(alice), Seq(cmd), Some(paintSynchronizerId))

        val paintOfferId = searchAcsSync(
          Seq(P4),
          paintSynchronizerAlias,
          PaintModule,
          OfferToPaintHouseByOwnerTemplate,
          stakeholder = Some(painter),
        )

        val sequencerPaint = getProgrammableSequencer("sequencer2")

        // When the confirmation result for the unassignment arrives, disconnect P5 from PaintSynchronizer
        val unassignmentResultReady = Promise[Unit]()
        val P5disconnected = Promise[Unit]()
        val paintMediator = mediator2.id
        sequencerPaint.setPolicy_("hold back confirmation result until P5 is disconnected") {
          submissionRequest =>
            if (submissionRequest.sender == paintMediator) {
              unassignmentResultReady.trySuccess(())
              SendDecision.HoldBack(P5disconnected.future)
            } else SendDecision.Process
        }

        val unassignedEventF = Future {
          P4.ledger_api.commands
            .submit_unassign(painter, paintOfferId, paintSynchronizerId, iouSynchronizerId)
            .unassignedEvent
        }
        val disconnectedF = unassignmentResultReady.future.map { _ =>
          P5.synchronizers.disconnect(paintSynchronizerAlias)
          P5disconnected.success(())
        }

        val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(2))
        val paintOfferUnassignedEvent = unassignedEventF.futureValue(patience, Position.here)
        disconnectedF.futureValue

        P4.ledger_api.commands.submit_assign(
          painter,
          paintOfferUnassignedEvent.unassignId,
          paintSynchronizerId,
          iouSynchronizerId,
        )

        logger.info("Reconnect P5")
        P5.synchronizers.reconnect(paintSynchronizerAlias)
        assertNotInAcsSync(
          Seq(P5),
          paintSynchronizerAlias,
          PaintModule,
          OfferToPaintHouseByOwnerTemplate,
          stakeholder = Some(alice),
        )
        assertNotInAcsSync(
          Seq(P4),
          paintSynchronizerAlias,
          PaintModule,
          OfferToPaintHouseByOwnerTemplate,
          stakeholder = Some(painter),
        )
      }
  }

}

//class SynchronizerChangeRealClockIntegrationTestDefault extends SynchronizerChangeRealClockIntegrationTest {
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer(
//        Seq(
//          Set(InstanceName.tryCreate("sequencer1")),
//          Set(InstanceName.tryCreate("sequencer2")),
//        )
//      ),
//    )
//  )
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class SynchronizerChangeRealClockIntegrationTestPostgres
    extends SynchronizerChangeRealClockIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
