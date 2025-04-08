// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.{ForceFlag, SynchronizerId}
import org.scalatest.matchers.{MatchResult, Matcher}

@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
trait MediatorTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S3M3_Manual

  private val synchronizer: SynchronizerAlias = SynchronizerAlias.tryCreate("bft-synchronizer1")

  private var synchronizerId: SynchronizerId = _
  var synchronizerOwners: Seq[LocalParticipantReference] = _

  s"Startup $synchronizer" in { implicit env =>
    import env.*
    // start the nodes that are configured in the config
    clue("starting up participants") {
      participants.local.start()
    }
    clue("start sequencers") {
      sequencers.local.start()
    }
    clue("start mediators") {
      mediators.local.start()
    }

    // bootstrap the synchronizers, using one of the synchronizer owner nodes as the coordinator
    val sequencersAll = Seq(sequencer1, sequencer2, sequencer3)
    val mediatorsAll = Seq(mediator1)
    // The synchronizer owners are the participants so that we can use their topology dispatcher to push topology changes to the synchronizer
    synchronizerOwners = Seq(participant1, participant2)

    synchronizerId = bootstrap.synchronizer(
      synchronizer.toProtoPrimitive,
      sequencers = sequencersAll,
      mediators = mediatorsAll,
      synchronizerOwners = synchronizerOwners,
      synchronizerThreshold = PositiveInt.two,
      staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
    )
  }

  "Connect synchronizer owners to the synchronizer" in { implicit env =>
    import env.*

    synchronizerOwners.foreach(_.synchronizers.connect_local(sequencer1, alias = synchronizer))
  }

  "Onboard new mediators" in { implicit env =>
    import env.*

    val med2Identity = mediator2.topology.transactions.list()
    val med3Identity = mediator3.topology.transactions.list()

    clue("onboarding mediator2") {
      clue(s"load $mediator2 identity") {
        synchronizerOwners.foreach(
          _.topology.transactions
            .load(
              med2Identity.result.map(_.transaction),
              store = synchronizerId,
              ForceFlag.AlienMember,
            )
        )
      }

      clue(s"onboard $mediator2") {
        synchronizerOwners
          .foreach(
            _.topology.mediators.propose(
              synchronizerId = synchronizerId,
              threshold = PositiveInt.one,
              active = Seq(mediator1, mediator2).map(_.id),
              observers = Seq.empty,
              group = NonNegativeInt.zero,
              serial = Some(PositiveInt.tryCreate(2)),
            )
          )
      }
    }

    clue("onboarding mediator3") {
      clue(s"load $mediator3 identity") {
        synchronizerOwners.foreach(
          _.topology.transactions
            .load(
              med3Identity.result.map(_.transaction),
              synchronizerId,
              ForceFlag.AlienMember,
            )
        )
      }

      clue(s"onboard $mediator3") {
        synchronizerOwners
          .foreach(
            _.topology.mediators.propose(
              synchronizerId = synchronizerId,
              threshold = PositiveInt.one.tryAdd(1),
              active = Seq(mediator1, mediator2, mediator3).map(_.id),
              observers = Seq.empty,
              group = NonNegativeInt.zero,
              serial = Some(PositiveInt.tryCreate(3)),
            )
          )
      }
    }
    eventually() {
      val mdsSerial = mediator1.topology.mediators
        .list(synchronizerId = synchronizerId)
        .map(_.context.serial)
        .headOption
      mdsSerial shouldBe Some(PositiveInt.tryCreate(3))
    }

    mediator2.setup.assign(
      synchronizerId,
      SequencerConnections.single(sequencer2.sequencerConnection),
    )
    mediator3.setup.assign(
      synchronizerId,
      SequencerConnections.single(sequencer3.sequencerConnection),
    )
  }

  s"Run ping on $synchronizer with BFT-mediators" in { implicit env =>
    import env.*

    // Connect participants for the synchronizer via "their" sequencers
    clue("participant1 connects to sequencer1") {
      participant1.synchronizers.disconnect(synchronizer)
      participant1.synchronizers.connect_local(sequencer1, alias = synchronizer)
    }
    clue("participant2 connects to sequencer2") {
      participant2.synchronizers.disconnect(synchronizer)
      participant2.synchronizers.connect_local(sequencer2, alias = synchronizer)
    }

    // Ensure that the group threshold is 2 for the test
    eventually() {
      val mediatorState1 =
        mediator1.topology.mediators.list(synchronizerId = synchronizerId).head
      mediatorState1.item.threshold shouldBe PositiveInt.tryCreate(2)
      val mediatorState2 =
        mediator2.topology.mediators.list(synchronizerId = synchronizerId).head
      mediatorState2.item.threshold shouldBe PositiveInt.tryCreate(2)
      val mediatorState3 =
        mediator3.topology.mediators.list(synchronizerId = synchronizerId).head
      mediatorState3.item.threshold shouldBe PositiveInt.tryCreate(2)
    }

    def containMessage(s: String): Matcher[LogEntry] = Matcher { (e: LogEntry) =>
      MatchResult(
        e.message.contains(s),
        s"log entry message did NOT contain $s",
        s"log entry message contains $s",
      )
    }

    // Run ping and assert on the logs
    def haveSignatureCount(n: Int): Matcher[LogEntry] = Matcher { (e: LogEntry) =>
      val regex = "Signature\\(signature =".r
      val matchCount = regex.findAllMatchIn(e.message).size
      MatchResult(
        matchCount == n,
        s"number of signatures $matchCount (actual) did NOT equal to $n (expected)",
        s"number of signatures $matchCount (actual) did EQUAL to $n (expected)",
      )
    }

    loggerFactory.assertLogsSeq(
      SuppressionRule.forLogger[TransactionProcessor]
        && SuppressionRule.LoggerNameContains("participant2")
    )(
      participant1.health.ping(participant2.id),
      atLeast(1, _) should (containMessage("ConfirmationResultMessage") and haveSignatureCount(2)),
    )

  }
}

class MediatorTestDefault extends MediatorTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

//class MediatorTestPostgres extends MediatorTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}
