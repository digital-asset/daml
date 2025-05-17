// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.{ForceFlag, PartyId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SynchronizerAlias, config}

import scala.jdk.CollectionConverters.*

trait ReassignmentTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5S4M4_Manual

  protected val sequencerGroups: MultiSynchronizer = MultiSynchronizer(
    Seq(
      Set(InstanceName.tryCreate("sequencer1"), InstanceName.tryCreate("sequencer2")),
      Set(InstanceName.tryCreate("sequencer3"), InstanceName.tryCreate("sequencer4")),
    )
  )

  private val synchronizer1: SynchronizerAlias = SynchronizerAlias.tryCreate("bft-synchronizer1")
  private val synchronizer2: SynchronizerAlias = SynchronizerAlias.tryCreate("bft-synchronizer2")

  private var synchronizerId1: SynchronizerId = _
  private var synchronizerId2: SynchronizerId = _
  private var synchronizerOwnersD2: NonEmpty[Seq[InstanceReference]] = _

  private var alice: PartyId = _
  private var bob: PartyId = _

  private var contractId: Iou.ContractId = _

  // Test uses `authority` keyword which requires LF 2.dev and protocol dev
  if (testedProtocolVersion >= ProtocolVersion.dev) {
    s"Startup $synchronizer1 and $synchronizer2" in { implicit env =>
      import env.*
      // STEP: start the nodes that are configured in the config
      clue("starting up participants") {
        participants.local.start()
      }
      clue("start sequencers") {
        sequencers.local.start()
      }
      clue("start mediators") {
        mediators.local.start()
      }

      // STEP: bootstrap the synchronizers, using one of the synchronizer owner nodes as the coordinator
      val sequencersD1 = Seq(sequencer1, sequencer2)
      val mediatorsD1 = Seq(mediator1, mediator2)
      // The synchronizer owners are the participants so that we can use their topology dispatcher to push topology changes to the synchronizer
      synchronizerOwnersD2 = NonEmpty(Seq, participant1, participant2)
      val sequencersD2 = NonEmpty(Seq, sequencer3, sequencer4)
      val mediatorsD2a = Seq(mediator3)

      synchronizerId1 = bootstrap.synchronizer(
        "bft-synchronizer1",
        sequencers = sequencersD1,
        mediators = mediatorsD1,
        synchronizerOwners = sequencersD1,
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )

      synchronizerId2 = bootstrap.synchronizer(
        "bft-synchronizer2",
        sequencers = sequencersD2,
        mediators = mediatorsD2a,
        synchronizerOwners = synchronizerOwnersD2,
        synchronizerThreshold = PositiveInt.one,
        staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
      )
    }

    s"Connect participants to $synchronizer1" in { implicit env =>
      import env.*

      // STEP: connect participants for the synchronizer via "their" sequencers
      clue("participant1 connects to sequencer1") {
        participant1.synchronizers.connect_local(sequencer1, alias = synchronizer1)
      }
      clue("participant2 connects to sequencer2") {
        participant2.synchronizers.connect_local(sequencer2, alias = synchronizer1)
      }

      // STEP: participants can now transact with each other
      participant1.health.ping(participant2.id)
    }

    "Allocate a party" in { implicit env =>
      import env.*
      alice = participant1.parties.enable(
        "alice",
        synchronizeParticipants = Seq(participant2),
        synchronizer = synchronizer1,
      )
      bob = participant2.parties.enable(
        "bob",
        synchronizeParticipants = Seq(participant1),
        synchronizer = synchronizer1,
      )
    }

    s"Participants can upload a dar and use it" in { implicit env =>
      import env.*

      val payer = alice
      val owner = bob

      clue("Upload dar") {
        Seq(participant1, participant2).foreach(_.dars.upload(CantonExamplesPath))
      }

      contractId = clue(s"create Iou contract on $synchronizer2") {
        val price = new Amount(100.toBigDecimal, "Coins")
        val createIouCmd = new Iou(
          payer.toProtoPrimitive,
          owner.toProtoPrimitive,
          price,
          List.empty.asJava,
        ).create.commands.asScala.toSeq
        val iou = clue("create-iou") {
          val tx = participant1.ledger_api.javaapi.commands.submit(
            Seq(payer),
            createIouCmd,
            commandId = "create-Iou",
          )
          JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement
        }
        iou.id
      }
    }

    s"Connect to a second synchronizer" in { implicit env =>
      import env.*

      clue(s"connect $participant1 to $synchronizer2") {
        participant1.synchronizers.connect_local(sequencer3, alias = synchronizer2)
      }
      clue(s"connect $participant2 to $synchronizer2") {
        participant2.synchronizers.connect_local(sequencer4, alias = synchronizer2)
      }
      participant1.parties.enable(
        "alice",
        synchronizeParticipants = Seq(participant2),
        synchronizer = synchronizer2,
      )
      participant2.parties.enable(
        "bob",
        synchronizeParticipants = Seq(participant1),
        synchronizer = synchronizer2,
      )

    }

    // We can add the new mediator group only after the participants have connected because they govern the synchronizer.
    s"Add another mediator group to $synchronizer2" in { implicit env =>
      import env.*

      val mediatorsD2b = Seq(mediator4)
      val medD2bIdentity = mediatorsD2b.flatMap(_.topology.transactions.identity_transactions())
      synchronizerOwnersD2.head.topology.transactions
        .load(medD2bIdentity, synchronizerId2, ForceFlag.AlienMember)

      // Add another mediator group for mediatorsD2b to synchronizer2
      synchronizerOwnersD2
        .foreach(node =>
          node.topology.mediators.propose(
            synchronizerId = synchronizerId2,
            threshold = PositiveInt.one,
            active = mediatorsD2b.map(_.id),
            observers = Seq.empty,
            group = NonNegativeInt.one,
            signedBy = Some(node.fingerprint),
          )
        )

      mediatorsD2b.foreach(mediator =>
        mediator.setup
          .assign(
            synchronizerId2.toPhysical,
            SequencerConnections.single(sequencer4.sequencerConnection),
          )
      )
    }

    "Reassign a contract" in { implicit env =>
      import env.*
      val payer = alice
      val owner = bob

      // Update the dynamic synchronizer parameters
      synchronizerOwnersD2.map { owner =>
        owner.topology.synchronizer_parameters.propose_update(
          synchronizerId = synchronizerId2,
          _.update(
            assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
            mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofMinutes(5),
          ),
          signedBy = Some(owner.fingerprint),
        )
      }

      // Obtain a new synchronizer timestamp for the reassignment's synchronizer time proof
      participant1.testing.fetch_synchronizer_time(synchronizerId2.toPhysical)

      val unassigned = clue(s"unassign from $synchronizer1 to $synchronizer2") {
        participant1.ledger_api.commands.submit_unassign(
          payer,
          Seq(contractId.toLf),
          synchronizerId1,
          synchronizerId2,
          submissionId = "unassignment-synchronizer2-synchronizer1",
        )
      }

      eventually() {
        inside {
          participant2.ledger_api.state.acs
            .incomplete_unassigned_of_party(payer)
        } { case Seq(incompleteUnassignedEvent) =>
          incompleteUnassignedEvent.contractId shouldBe contractId.toLf.coid
        }
      }

      clue(s"assignment on $synchronizer1") {
        participant2.ledger_api.commands.submit_assign(
          owner,
          unassigned.unassignId,
          synchronizerId1,
          synchronizerId2,
          submissionId = "assignment-synchronizer2-synchronizer1",
        )
      }

      clue(s"use the contract on $synchronizer1") {
        val exerciseCallCmd = contractId.exerciseCall().commands.asScala.toSeq
        participant2.ledger_api.javaapi.commands.submit(
          Seq(owner),
          exerciseCallCmd,
          commandId = "exercise-Iou",
        )
      }
    }
  }
}

//class ReassignmentTestDefault extends ReassignmentTest {
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory, sequencerGroups)
//  )
//}

class ReassignmentTestPostgres extends ReassignmentTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory, sequencerGroups)
  )
  registerPlugin(new UsePostgres(loggerFactory))
}
