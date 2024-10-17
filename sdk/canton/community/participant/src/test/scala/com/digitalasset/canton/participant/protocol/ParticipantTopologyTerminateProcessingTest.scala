// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
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
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, RequestCounter, SequencerCounter}
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class ParticipantTopologyTerminateProcessingTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  protected def mkStore: TopologyStore[TopologyStoreId.DomainStore] = new InMemoryTopologyStore(
    TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
    loggerFactory,
    timeouts,
  )

  private def mk(
      store: TopologyStore[TopologyStoreId.DomainStore] = mkStore
  ): (
      ParticipantTopologyTerminateProcessing,
      TopologyStore[TopologyStoreId.DomainStore],
      ArgumentCaptor[Option[Traced[Update]]],
      RecordOrderPublisher,
  ) = {

    val eventCaptor: ArgumentCaptor[Option[Traced[Update]]] =
      ArgumentCaptor.forClass(classOf[Option[Traced[Update]]])

    val recordOrderPublisher = mock[RecordOrderPublisher]
    when(
      recordOrderPublisher.tick(
        any[SequencerCounter],
        any[CantonTimestamp],
        eventCaptor.capture(),
        any[Option[RequestCounter]],
      )(any[TraceContext])
    )
      .thenReturn(Future.successful(()))

    val proc = new ParticipantTopologyTerminateProcessing(
      DefaultTestIdentities.domainId,
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
  ): Future[Unit] =
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

        for {
          _ <- add(store, cts, List(party1participant1))
          _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts))
        } yield {
          verify(rop, times(1)).tick(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Option[Traced[Update]]],
            any[Option[RequestCounter]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case Some(Traced(TopologyTransactionEffective(_, events, _, _, _, _))) =>
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
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        } yield {
          verify(rop, times(1)).tick(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Option[Traced[Update]]],
            any[Option[RequestCounter]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case Some(Traced(TopologyTransactionEffective(_, events, _, _, _, _))) =>
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
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        } yield {
          verify(rop, times(1)).tick(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Option[Traced[Update]]],
            any[Option[RequestCounter]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          forAll(events) {
            case Some(Traced(TopologyTransactionEffective(_, events, _, _, _, _))) =>
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
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        } yield {
          verify(rop, times(1)).tick(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Option[Traced[Update]]],
            any[Option[RequestCounter]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          events.head shouldBe None
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
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        } yield {
          verify(rop, times(1)).tick(
            any[SequencerCounter],
            any[CantonTimestamp],
            any[Option[Traced[Update]]],
            any[Option[RequestCounter]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 1
          events.head shouldBe None
        }
      }
    }
  }

}
