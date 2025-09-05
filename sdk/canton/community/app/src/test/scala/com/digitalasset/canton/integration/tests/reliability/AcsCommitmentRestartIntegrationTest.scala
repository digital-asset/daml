// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.{LedgerParticipantId, config}
import monocle.Monocle.toAppliedFocusOps

import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.util.Using

trait AcsCommitmentRestartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils {

  private lazy val reconciliationInterval = PositiveSeconds.tryOfSeconds(60)
  private lazy val confirmationResponseTimeout = Duration.ofMinutes(1)
  private lazy val mediatorReactionTimeout = Duration.ofHours(1)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
      )
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            confirmationResponseTimeout =
              config.NonNegativeFiniteDuration(confirmationResponseTimeout),
            reconciliationInterval = reconciliationInterval.toConfig,
            mediatorReactionTimeout = config.NonNegativeFiniteDuration(mediatorReactionTimeout),
            sequencerAggregateSubmissionTimeout = config
              .NonNegativeFiniteDuration(confirmationResponseTimeout.plus(mediatorReactionTimeout)),
          ),
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
      }

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private var preCommitmentTs: CantonTimestamp = _

  private def withLedgerApiStoreFor[T](
      participant: LocalParticipantReference
  )(code: LedgerApiStore => T)(implicit ec: ExecutionContextIdlenessExecutorService): T =
    Using.resource(
      LedgerApiStore
        .initialize(
          storageConfig = participant.config.storage,
          ledgerParticipantId = LedgerParticipantId.assertFromString("fakeid"),
          legderApiDatabaseConnectionTimeout = LedgerApiServerConfig().databaseConnectionTimeout,
          ledgerApiPostgresDataSourceConfig = PostgresDataSourceConfig(),
          timeouts = timeouts,
          loggerFactory = loggerFactory,
          metrics = ParticipantTestMetrics.ledgerApiServer,
        )
        .futureValueUS
    )(code)

  "create a shared contract between two participants and trigger an ACS commitment" in {
    implicit env =>
      import env.*

      val simClock = environment.simClock.value
// Advance the sim clock so that we can be sure that the reconciliation interval of one minute is used
      simClock.advance(java.time.Duration.ofDays(1))
      val start = simClock.uniqueTime()
      preCommitmentTs = start

      val createIouCmd = new Iou(
        participant1.adminParty.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
        new Amount(100.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq
      participant1.ledger_api.javaapi.commands
        .submit(Seq(participant1.adminParty), createIouCmd)

// Advance the clock and let the participants exchange a ping.
      simClock.advance(reconciliationInterval.duration.plusMillis(1))
      participant2.health.ping(participant1)
      val stop = simClock.uniqueTime()

      eventually() {
        val commitmentsFromP2 = participant1.commitments.received(
          daName,
          start.toInstant,
          stop.toInstant,
          Some(participant2),
        )
        commitmentsFromP2 should have size 1
      }
  }

  // After participant 1 computes and sends a commitment, the test moves back the ledger-end and synchronizer-indexes to scratch.
  // By moving the synchronizer index backwards, crash recovery of the ACS commitment processor will reprocess
  // requests starting from the clean sequencer counter, and we want that the ACS commitment processor on participant 1
  // ignores the replayed requests. So no warning from the ACS commitment processor is good news.
  "test idempotency of the ACS commitment processor" in { implicit env =>
    import env.*
    val simClock = environment.simClock.value

    // wait that participant2 receives participant1's commitment, so that we know participant 1 doesn't need to resend
    // the commitment
    eventually() {
      val commitmentsFromP1 = participant2.commitments.received(
        daName,
        preCommitmentTs.toInstant,
        simClock.uniqueTime().toInstant,
        Some(participant1),
      )
      commitmentsFromP1 should have size 1
    }

    // restart the participant and reset the ledger-ends
    participant1.synchronizers.disconnect(daName)
    participant1.stop()
    withLedgerApiStoreFor(participant1)(
      _.moveLedgerEndBackToScratch().futureValueUS
    )
    val stop = simClock.uniqueTime()
    loggerFactory.assertLogs(
      {
        participant1.start()
        participant1.synchronizers.reconnect(daName)

        // perform a ping to trigger p1 send a commitment
        participant2.health.ping(participant1)
      },
      // this warning message is a byproduct of re-processing all the messages for the synchronizer, as starting points moved back to scratch
      _.warningMessage should startWith regex "Response message for request .* timed out at",
    )

    // participant 1 doesn't publish the changes
    // if it did, then the running commitment would be incorrect and the computed commitment wrong, leading to a DB conflict
    val (computedByP1, commitmentsFromP2) = always() {
      // check that participant1 compute other commitments for a while => therefore there must have been no call of publish
      val computedByP1 = participant1.commitments.computed(
        daName,
        preCommitmentTs.toInstant,
        stop.toInstant,
        Some(participant2),
      )
      computedByP1 should have size 1

      val commitmentsFromP2 = participant1.commitments.received(
        daName,
        preCommitmentTs.toInstant,
        stop.toInstant,
        Some(participant2),
      )
      commitmentsFromP2 should have size 1
      (computedByP1, commitmentsFromP2)
    }

    // the commitment p1 had already computed before is the same as the commitment received from p2
    commitmentsFromP2.map(_.message.commitment) shouldBe computedByP1.map { case (_, _, computed) =>
      computed
    }

    // participant 2 doesn't receive a new commitment, because participant 1 does not recompute a commitment
    // therefore, the number of received commitments by participant 2 is still 1
    always() {
      val commitmentsFromP1 = participant2.commitments.received(
        daName,
        preCommitmentTs.toInstant,
        stop.toInstant,
        Some(participant1),
      )
      commitmentsFromP1 should have size 1
    }
  }

  // TODO(i19694): Extend test coverage to: forcing recovery of the ACS Commitment processor, testing ACS Commitment with successful and not-succesful repair operations.
}

//class AcsCommitmentRestartIntegrationTestH2 extends AcsCommitmentRestartIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class AcsCommitmentRestartIntegrationTestPostgres extends AcsCommitmentRestartIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
