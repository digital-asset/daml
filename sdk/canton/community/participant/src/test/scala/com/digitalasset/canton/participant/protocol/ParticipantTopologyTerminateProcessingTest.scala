// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.SequencedUpdate
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.{
  SequencerIndexMoved,
  TopologyTransactionEffective,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionTestFactory,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, SequencerCounter}
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class ParticipantTopologyTerminateProcessingTest
    extends AsyncWordSpec
    with BaseTest
    with FailOnShutdown
    with HasExecutionContext {

  protected def mkStore: TopologyStore[TopologyStoreId.DomainStore] = new InMemoryTopologyStore(
    TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
    testedProtocolVersion,
    loggerFactory,
    timeouts,
  )

  private def mk(
      store: TopologyStore[TopologyStoreId.DomainStore] = mkStore
  ): (
      ParticipantTopologyTerminateProcessing,
      TopologyStore[TopologyStoreId.DomainStore],
      ArgumentCaptor[SequencedUpdate],
      RecordOrderPublisher,
  ) = {

    val eventCaptor: ArgumentCaptor[SequencedUpdate] =
      ArgumentCaptor.forClass(classOf[SequencedUpdate])

    val recordOrderPublisher = mock[RecordOrderPublisher]
    when(
      recordOrderPublisher.tick(eventCaptor.capture())(any[TraceContext])
    )
      .thenReturn(Future.successful(()))

    val proc = new ParticipantTopologyTerminateProcessing(
      DefaultTestIdentities.domainId,
      testedProtocolVersion,
      recordOrderPublisher,
      store,
      loggerFactory,
    )
    (proc, store, eventCaptor, recordOrderPublisher)
  }

  val factory = new TestingOwnerWithKeys(
    DefaultTestIdentities.participant1,
    loggerFactory,
    parallelExecutionContext,
  )

  import DefaultTestIdentities.*
  import factory.*

  lazy val party1participant1 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Confirmation)),
    )
  )
  lazy val party1participant1_2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Submission),
      ),
    )
  )
  lazy val party1participant1_2_threshold2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.two,
      Seq(
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Submission),
      ),
    )
  )

  def add(
      store: TopologyStore[TopologyStoreId.DomainStore],
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- store.update(
        SequencedTime(timestamp),
        EffectiveTime(timestamp),
        removeMapping = transactions.map(tx => tx.mapping.uniqueKey -> tx.serial).toMap,
        removeTxs = transactions.map(_.hash).toSet,
        additions = transactions.map(ValidatedTopologyTransaction(_)),
      )
    } yield ()

  private def timestampWithCounter(idx: Int): (CantonTimestamp, SequencerCounter) =
    CantonTimestamp.Epoch.plusSeconds(idx.toLong) -> SequencerCounter(idx)

  val EventsEnabledSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[ParticipantTopologyTerminateProcessing] && SuppressionRule
      .LevelAndAbove(Level.WARN)

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  ParticipantTopologyTerminateProcessing.getClass.getSimpleName should {
    "notify of the party rights grant to the first participant" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts, sc) = timestampWithCounter(0)

        val txs = List(party1participant1)

        for {
          _ <- add(store, cts, txs)
          _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts), txs)
        } yield {
          verify(rop, times(1)).tick(any[SequencedUpdate])(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _, _, _) =>
              forAll(events) {
                case PartyToParticipantAuthorization(
                      party,
                      participant,
                      AuthorizationLevel.Submission,
                    ) =>
                  party shouldBe party1.toLf
                  participant shouldBe participant1.uid.toProtoPrimitive
                case PartyToParticipantAuthorization(_, _, _) =>
                  fail("unexpected topology event")
              }
            case _ => fail("unexpected transaction type")
          }
        }
      }
    }

    "notify of the party rights grant to the second participant" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts0, _) = timestampWithCounter(0)
        val (cts1, sc1) = timestampWithCounter(1)

        for {
          _ <- add(store, cts0, List(party1participant1))
          _ <- add(store, cts1, List(party1participant1_2))
          _ <- proc.terminate(
            sc1,
            SequencedTime(cts1),
            EffectiveTime(cts1),
            List(party1participant1_2),
          )
        } yield {
          verify(rop, times(1)).tick(any[SequencedUpdate])(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _, _, _) =>
              forAll(events) {
                case PartyToParticipantAuthorization(
                      party,
                      participant,
                      AuthorizationLevel.Submission,
                    ) =>
                  party shouldBe party1.toLf
                  participant shouldBe participant2.uid.toProtoPrimitive
                case PartyToParticipantAuthorization(_, _, _) =>
                  fail("unexpected topology event")
              }
            case _ => fail("unexpected transaction type")
          }
        }
      }
    }

    "notify of the party rights revocation from the second participant" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts0, _) = timestampWithCounter(0)
        val (cts1, sc1) = timestampWithCounter(1)

        for {
          _ <- add(store, cts0, List(party1participant1_2))
          _ <- add(store, cts1, List(party1participant1))
          _ <- proc.terminate(
            sc1,
            SequencedTime(cts1),
            EffectiveTime(cts1),
            List(party1participant1),
          )
        } yield {
          verify(rop, times(1)).tick(any[SequencedUpdate])(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _, _, _) =>
              forAll(events) {
                case PartyToParticipantAuthorization(
                      party,
                      participant,
                      AuthorizationLevel.Revoked,
                    ) =>
                  party shouldBe party1.toLf
                  participant shouldBe participant2.uid.toProtoPrimitive
                case PartyToParticipantAuthorization(_, _, _) =>
                  fail("unexpected topology event")
              }
            case _ => fail("unexpected transaction type")
          }
        }
      }
    }
    "tick empty if the party rights threshold change" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts0, _) = timestampWithCounter(0)
        val (cts1, sc1) = timestampWithCounter(1)

        for {
          _ <- add(store, cts0, List(party1participant1_2))
          _ <- add(store, cts1, List(party1participant1_2_threshold2))
          _ <- proc.terminate(
            sc1,
            SequencedTime(cts1),
            EffectiveTime(cts1),
            List(party1participant1_2_threshold2),
          )
        } yield {
          verify(rop, times(1)).tick(any[SequencedUpdate])(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          events.head match {
            case _: SequencerIndexMoved => succeed
            case _ => fail("SequencerIndexMoved expected")
          }
        }
      }
    }

    "tick empty if no change" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts0, _) = timestampWithCounter(0)
        val (cts1, sc1) = timestampWithCounter(1)

        for {
          _ <- add(store, cts0, List(party1participant1_2))
          _ <- add(store, cts1, List.empty)
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1), List.empty)
        } yield {
          verify(rop, times(1)).tick(any[SequencedUpdate])(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          events.head match {
            case _: SequencerIndexMoved => succeed
            case _ => fail("SequencerIndexMoved expected")
          }
        }
      }
    }
  }

}
