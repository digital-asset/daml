// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.memory.InMemoryPartyMetadataStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, LedgerParticipantId, SequencerCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

final class LedgerServerPartyNotifierTest extends AsyncWordSpec with BaseTest {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*
  import com.digitalasset.canton.topology.client.EffectiveTimeTestHelpers.*

  private lazy val crypto =
    new TestingOwnerWithKeys(sequencerId, loggerFactory, directExecutionContext)

  private object Fixture {
    def apply(test: Fixture => Future[Assertion]): Future[Assertion] =
      test(new Fixture)
  }

  private final class Fixture {
    private val store = new InMemoryPartyMetadataStore()
    private val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    private val observedEvents = ListBuffer[Update]()
    private val eventPublisher = mock[ParticipantEventPublisher]

    val notifier: LedgerServerPartyNotifier =
      new LedgerServerPartyNotifier(
        participant1,
        eventPublisher,
        store,
        clock,
        FutureSupervisor.Noop,
        mustTrackSubmissionIds = false,
        exitOnFatalFailures = true,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

    private val subscriber = notifier.attachToTopologyProcessor()

    private var counter = SequencerCounter(0)

    def simulateTransaction(mapping: PartyToParticipant): Future[Unit] = {
      val tx: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant] =
        TopologyTransaction(
          TopologyChangeOp.Replace,
          PositiveInt.one,
          mapping,
          testedProtocolVersion,
        )

      val txs = Seq(crypto.mkTrans(tx)(directExecutionContext))
      val now = clock.now
      val result = subscriber
        .observed(now, now, counter, txs)
        .onShutdown(
          logger.debug(
            "Tests are shutting down before the simulated transaction could be processed"
          )
        )
      clock.advanceTo(now.immediateSuccessor)
      counter += 1
      result
    }

    def simulateTransaction(
        partyId: PartyId,
        participantId: ParticipantId,
    ): Future[Unit] =
      simulateTransaction(
        PartyToParticipant.tryCreate(
          partyId,
          PositiveInt.one,
          Seq(HostingParticipant(participantId, ParticipantPermission.Submission)),
        )
      )

    when(eventPublisher.publishEventDelayableByRepairOperation(any[Update])(anyTraceContext))
      .thenAnswer { (update: Update) =>
        observedEvents += update
        FutureUnlessShutdown.unit
      }

    def expectLastObserved(
        expectedPartyId: PartyId,
        expectedDisplayName: String,
        expectedParticipantId: String,
    ): Assertion = {
      observedEvents should not be empty
      inside(observedEvents.last) { case event: Update.PartyAddedToParticipant =>
        event.party shouldBe expectedPartyId.toLf
        event.displayName shouldBe expectedDisplayName
        event.participantId shouldBe LedgerParticipantId.assertFromString(expectedParticipantId)
      }
    }

    def observed: List[Update] = observedEvents.toList

  }

  "ledger server notifier" should {

    "update party to participant mappings" in Fixture { scenario =>
      for {
        _ <- scenario.simulateTransaction(party1, participant1)
      } yield scenario.expectLastObserved(party1, "", participant1.uid.toProtoPrimitive)
    }

    "combine name and ids" in Fixture { fixture =>
      val displayName = String255.tryCreate("TestMe")
      for {
        _ <- fixture.simulateTransaction(party1, participant1)
        _ <- fixture.notifier.setDisplayName(party1, displayName)
      } yield fixture.expectLastObserved(
        party1,
        displayName.unwrap,
        participant1.uid.toProtoPrimitive,
      )
    }

    "add admin parties" in Fixture { fixture =>
      for {
        _ <- fixture.simulateTransaction(
          PartyToParticipant.tryCreate(
            participant1.adminParty,
            PositiveInt.one,
            Seq(HostingParticipant(participant1, ParticipantPermission.Submission)),
          )
        )
      } yield fixture.expectLastObserved(
        participant1.adminParty,
        "",
        participant1.uid.toProtoPrimitive,
      )
    }

    "prefer current participant" in Fixture { fixture =>
      for {
        _ <- fixture.simulateTransaction(party1, participant1)
        _ <- fixture.simulateTransaction(party1, participant2)
      } yield fixture.expectLastObserved(party1, "", participant1.uid.toProtoPrimitive)

    }

    "not send duplicate updates" in Fixture { fixture =>
      for {
        _ <- fixture.simulateTransaction(party1, participant1)
        _ <- fixture.simulateTransaction(party1, participant1)
      } yield fixture.observed should have length 1

    }

  }

}
