// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  InformeesNotActive,
  NoCommonSynchronizer,
}
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

sealed trait GeneralSynchronizerRouterIntegrationTest
    extends SynchronizerRouterIntegrationTestSetup {

  import SynchronizerRouterIntegrationTestSetup.*

  override def environmentDefinition: EnvironmentDefinition = super.environmentDefinition
    .withSetup { implicit env =>
      import env.*
      participants.all.dars.upload(darPath)

      connectToDefaultSynchronizers()
    }

  "the auto reassignment transactions should fail gracefully" when {
    "there is no common synchronizer" in { implicit env =>
      import env.*

      connectToCustomSynchronizers(
        Map(
          participant2 -> Set(synchronizer2, synchronizer3),
          participant3 -> Set(synchronizer3),
          participant4 -> Set(synchronizer2),
        )
      )

      val party3C = createSingle(party3Id, Some(synchronizer3Id), participant3, party2Id).id
      val party4C = createSingle(party4Id, Some(synchronizer2Id), participant4, party2Id).id
      val contractIds = List(party3C, party4C)

      val aggregate = createAggregate(participant2, party2Id, contractIds, Some(synchronizer2Id))
      val aggregateId = aggregate.id.toLf

      assertInAcsSync(List(participant2), synchronizer3, party3C.toLf)
      assertInAcsSync(List(participant2), synchronizer2, party4C.toLf)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        exerciseCountAll(participant2, party2Id, aggregate),
        x => x.commandFailureMessage should include(InformeesNotActive.id),
      )

      // No contracts have moved
      assertInAcsSync(List(participant2), synchronizer2, aggregateId)
      assertInAcsSync(List(participant2), synchronizer3, party3C.toLf)
      assertInAcsSync(List(participant2), synchronizer2, party4C.toLf)
    }

    "fails if there is no common synchronizer to which the submitter can submit and all informees are connected" in {
      implicit env =>
        import env.*

        connectToCustomSynchronizers(
          Map(participant2 -> Set(synchronizer2, synchronizer3), participant4 -> Set(synchronizer2))
        )

        sequencer2.topology.participant_synchronizer_permissions
          .propose(synchronizer2Id, participant2.id, ParticipantPermission.Observation)
        synchronizeTopologyState()

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          createSingle(party2Id, None, participant2, party4Id).id,
          x => x.commandFailureMessage should include(NoCommonSynchronizer.id),
        )
        // set the permission back to submission
        sequencer2.topology.participant_synchronizer_permissions
          .propose(synchronizer2Id, participant2.id, ParticipantPermission.Submission)
        synchronizeTopologyState()
    }

    "the synchronizer specified in the workflow id cannot be reassigned to" in { implicit env =>
      import env.*

      connectToDefaultSynchronizers()

      val singleId = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id
      val contractIds = List(singleId)

      val aggregate = createAggregate(participant1, party1Id, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf
      assertInAcsSync(List(participant1), synchronizer1, aggregateId)

      assertInAcsSync(List(participant1), synchronizer2, singleId.toLf)
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        exerciseCountAll(participant1, party1Id, aggregate, Some(synchronizer1Id)),
        _.commandFailureMessage should include(InvalidPrescribedSynchronizerId.id),
      )

      assertInAcsSync(List(participant1), synchronizer1, aggregateId)
      assertInAcsSync(List(participant2), synchronizer2, singleId.toLf)
    }
  }

  "the auto reassignment transactions, in the happy case" should {

    "reassign the Aggregate contract to the synchronizer of the Single contract" in {
      implicit env =>
        import env.*

        connectToDefaultSynchronizers()

        val singleId = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id

        val contractIds = List(singleId)

        val aggregate = createAggregate(participant1, party1Id, contractIds, Some(synchronizer1Id))
        val aggregateId = aggregate.id

        assertInAcsSync(List(participant1), synchronizer1, aggregateId.toLf)
        assertInAcsSync(List(participant1), synchronizer2, singleId.toLf)
        exerciseCountAll(participant1, party1Id, aggregate)

        assertInAcsSync(List(participant1), synchronizer2, aggregateId.toLf)
        assertInAcsSync(List(participant2), synchronizer2, singleId.toLf)
    }

    "reassign multiple contracts" in { implicit env =>
      import env.*

      connectToDefaultSynchronizers()

      val createP2 = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id
      val createP3 = createSingle(party3Id, Some(synchronizer3Id), participant3, party1Id).id
      val contractIds = List(createP2, createP3).reverse

      val aggregate = createAggregate(participant1, party1Id, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf

      assertInAcsSync(List(participant1), synchronizer2, createP2.toLf)
      assertInAcsSync(List(participant1), synchronizer3, createP3.toLf)
      exerciseCountAll(participant1, party1Id, aggregate).discard

      assertInAcsSync(List(participant1), synchronizer3, aggregateId)
      assertInAcsSync(List(participant2), synchronizer3, createP2.toLf)
      assertInAcsSync(List(participant3), synchronizer3, createP3.toLf)
    }
    "pick synchronizer3 as specified by the workflow id" in { implicit env =>
      import env.*

      connectToDefaultSynchronizers()

      val singleId = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id
      val contractIds = List(singleId)

      val aggregate = createAggregate(participant1, party1Id, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf

      assertInAcsSync(List(participant1), synchronizer2, singleId.toLf)
      exerciseCountAll(participant1, party1Id, aggregate, Some(synchronizer3Id))

      assertInAcsSync(List(participant1), synchronizer3, aggregateId)
      assertInAcsSync(List(participant2), synchronizer3, singleId.toLf)
    }
    "pick the synchronizer requiring the fewest reassignments" in { implicit env =>
      import env.*

      connectToDefaultSynchronizers()

      val singleId = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id
      val singleIds =
        (1 to 4).map(_ => createSingle(party2Id, Some(synchronizer3Id), participant2, party1Id).id)
      val contractIds = singleId +: singleIds

      val aggregate =
        createAggregate(participant1, party1Id, contractIds.toList, Some(synchronizer2Id))
      val aggregateId = aggregate.id.toLf

      assertInAcsSync(List(participant1), synchronizer2, singleId.toLf)
      singleIds foreach { c =>
        assertInAcsSync(List(participant1), synchronizer3, c.toLf)
      }
      exerciseCountAll(participant1, party1Id, aggregate)

      assertInAcsSync(List(participant1), synchronizer3, aggregateId)
      contractIds foreach { c =>
        assertInAcsSync(List(participant2), synchronizer3, c.toLf)
      }
    }
    "pick the synchronizer where all informees hosted when exercising" in { implicit env =>
      import env.*

      connectToDefaultSynchronizers()

      val inject = createInject(party1Id, Some(synchronizer1Id), participant1)
      val newInject = exerciseInForm(participant1, party1Id, party2Id, inject)
      assertInAcsSync(List(participant2), synchronizer2, newInject.id.toLf)
    }

    "pick a common synchronizer even when no contract resides on it" in { implicit env =>
      import env.*

      connectToCustomSynchronizers(
        Map(
          participant1 -> Set(synchronizer1, synchronizer3),
          participant2 -> Set(synchronizer2, synchronizer3),
          participant3 -> Set(synchronizer1, synchronizer2, synchronizer3),
        )
      )

      val party1C = createSingle(party1Id, Some(synchronizer1Id), participant1, party3Id).id
      val party2C = createSingle(party2Id, Some(synchronizer2Id), participant2, party3Id).id
      val contractIds = List(party1C, party2C)

      val aggregate = createAggregate(participant3, party3Id, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf

      assertInAcsSync(List(participant3), synchronizer1, party1C.toLf)
      assertInAcsSync(List(participant3), synchronizer2, party2C.toLf)
      assertInAcsSync(List(participant3), synchronizer1, aggregateId)

      exerciseCountAll(participant3, party3Id, aggregate)

      assertInAcsSync(List(participant3), synchronizer3, aggregateId)
      assertInAcsSync(List(participant1), synchronizer3, party1C.toLf)
      assertInAcsSync(List(participant2), synchronizer3, party2C.toLf)
    }

    "pick the synchronizer for contract creation where the participant has submission permission" in {
      implicit env =>
        import env.*

        connectToCustomSynchronizers(
          Map(
            participant1 -> Set(synchronizer1, synchronizer2, synchronizer3),
            participant2 -> Set(synchronizer2, synchronizer3),
          )
        )

        val c1 = createSingle(party2Id, None, participant2, party1Id).id
        assertInAcsSync(List(participant2), synchronizer2, c1.toLf)

        sequencer2.topology.participant_synchronizer_permissions
          .propose(synchronizer2Id, participant2.id, ParticipantPermission.Observation)

        eventually() {
          participant2.topology.participant_synchronizer_permissions
            .find(synchronizer2Id, participant2.id)
            .value
            .item
            .permission shouldBe ParticipantPermission.Observation
        }

        val c2 = createSingle(party2Id, None, participant2, party1Id).id
        assertInAcsSync(List(participant2), synchronizer3, c2.toLf)

        sequencer2.topology.participant_synchronizer_permissions
          .propose(synchronizer2Id, participant2.id, ParticipantPermission.Submission)

        eventually() {
          participant2.topology.participant_synchronizer_permissions
            .find(synchronizer2Id, participant2.id)
            .value
            .item
            .permission shouldBe ParticipantPermission.Submission
        }

        synchronizeTopologyState()

        val c3 = createSingle(party2Id, None, participant2, party1Id).id
        assertInAcsSync(List(participant2), synchronizer2, c3.toLf)
    }

    "pick the synchronizer with the highest priority" in { implicit env =>
      import env.*

      connectToCustomSynchronizers(Map(participant1 -> Set(synchronizer1, synchronizer2)))

      // Increase the priority of synchronizer2 to 1.
      participant1.synchronizers.modify(synchronizer2, _.focus(_.priority).replace(1))

      val singleId = createSingle(party1Id, Some(synchronizer2Id), participant1, party1aId).id
      val contractIds = List(singleId)

      val aggregate = createAggregate(participant1, party1aId, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf

      exerciseCountAll(participant1, party1aId, aggregate)

      assertInAcsSync(List(participant1), synchronizer2, aggregateId)
      assertInAcsSync(List(participant1), synchronizer2, singleId.toLf)

      // Make sure the priority of synchronizer2 is reset to 0.
      participant1.synchronizers.modify(synchronizer2, _.focus(_.priority).replace(0))
    }

    "pick the synchronizer that is alphabetically first" in { implicit env =>
      import env.*

      connectToCustomSynchronizers(Map(participant1 -> Set(synchronizer1, synchronizer2)))

      val singleId = createSingle(party1Id, Some(synchronizer2Id), participant1, party1aId).id
      val contractIds = List(singleId)

      val aggregate = createAggregate(participant1, party1aId, contractIds, Some(synchronizer1Id))
      val aggregateId = aggregate.id.toLf

      exerciseCountAll(participant1, party1aId, aggregate)

      assertInAcsSync(List(participant1), synchronizer1, aggregateId)
      assertInAcsSync(List(participant1), synchronizer1, singleId.toLf)
    }

    "reassign the contracts of a multi-party submission to a common synchronizer" in {
      implicit env =>
        import env.*

        connectToCustomSynchronizers(Map(participant1 -> Set(synchronizer1, synchronizer2)))

        def testMultiSubmitterSynchronizerRouting(
            targetSynchronizer: Option[SynchronizerAlias]
        ): Unit = {
          val contractTopology =
            Map(party1aId -> synchronizer1, party1bId -> synchronizer2, party1cId -> synchronizer2)

          val dummies = contractTopology.map { case (party, synchronizer) =>
            val synchronizerId = initializedSynchronizers(synchronizer).synchronizerId
            val dummy = createDummy(party, Some(synchronizerId), participant1)
            assertInAcsSync(Seq(participant1), synchronizer, dummy.id.toLf)
            dummy
          }
          val synchronizerId =
            targetSynchronizer.flatMap(initializedSynchronizers.get(_).map(_.synchronizerId))
          val events =
            exerciseDummies(
              contractTopology.keys,
              dummies,
              participant1,
              synchronizerId,
            ).getEventsById.asScala.toSeq
              .sortBy(_._1)
              .map(_._2)

          contractTopology.values
            .toSet[SynchronizerAlias]
            .foreach(synchronizer =>
              assertNotInAcsSync(Seq(participant1), synchronizer, "Test", "Dummy")
            )

          events.size shouldBe contractTopology.size + 1
          events
            .take(contractTopology.size)
            .foreach(ex => assert(ex.toProtoEvent.hasExercised, "expect exercise"))

          assert(
            events.lastOption.value.toProtoEvent.hasCreated,
            "expect last event to be creation of Single",
          )

          // When not target synchronizer is specified expect the synchronizer with the maximum number of input contracts to determine synchronizer.
          val expectedRoutingSynchronizer = targetSynchronizer.getOrElse {
            contractTopology.values.groupBy(identity).maxBy(_._2.size)._1
          }

          assertInAcsSync(
            Seq(participant1),
            expectedRoutingSynchronizer,
            LfContractId.assertFromString(
              events.lastOption.value.toProtoEvent.getCreated.getContractId
            ),
          )
        }
        // No target synchronizer tests code path choosing optimal synchronizer (B)
        testMultiSubmitterSynchronizerRouting(targetSynchronizer = None)
        // Test code path requesting non-optimal synchronizer (A)
        testMultiSubmitterSynchronizerRouting(Some(synchronizer1))
    }

    "a target synchronizer different than the synchronizer of the input contracts results in a multi-synchronizer transaction" in {
      implicit env =>
        import env.*

        connectToCustomSynchronizers(Map(participant1 -> Set(synchronizer1, synchronizer2)))
        val contractIds =
          List(createSingle(party1Id, Some(synchronizer1Id), participant1, party1Id).id)
        val aggregate = createAggregate(participant1, party1Id, contractIds, Some(synchronizer1Id))
        exerciseCountAll(participant1, party1Id, aggregate, Some(synchronizer2Id))
        assertInAcsSync(List(participant1), synchronizer2, aggregate.id.toLf)
    }
  }
}

//class GeneralSynchronizerRouterIntegrationTestDefault extends GeneralSynchronizerRouterIntegrationTest {

class GeneralSynchronizerRouterIntegrationTestPostgres
    extends GeneralSynchronizerRouterIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"), Set("sequencer3"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
}
