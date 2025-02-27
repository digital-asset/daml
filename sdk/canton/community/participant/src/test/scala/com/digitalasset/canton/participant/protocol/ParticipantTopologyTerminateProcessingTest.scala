// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.{FloatingUpdate, Update}
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
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  SequencerCounter,
}
import org.mockito.ArgumentCaptor
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.*

class ParticipantTopologyTerminateProcessingTest
    extends AsyncWordSpec
    with BaseTest
    with FailOnShutdown
    with HasExecutionContext {

  protected def mkStore: TopologyStore[TopologyStoreId.SynchronizerStore] =
    new InMemoryTopologyStore(
      TopologyStoreId.SynchronizerStore(DefaultTestIdentities.synchronizerId),
      testedProtocolVersion,
      loggerFactory,
      timeouts,
    )

  private def mk(
      store: TopologyStore[TopologyStoreId.SynchronizerStore] = mkStore,
      initialRecordTime: CantonTimestamp = CantonTimestamp.MinValue,
  ): (
      ParticipantTopologyTerminateProcessing,
      TopologyStore[TopologyStoreId.SynchronizerStore],
      ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]],
      RecordOrderPublisher,
  ) = {

    val eventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
      ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])

    val recordOrderPublisher = mock[RecordOrderPublisher]
    when(
      recordOrderPublisher.scheduleFloatingEventPublication(
        any[CantonTimestamp],
        eventCaptor.capture(),
      )(any[TraceContext])
    )
      .thenReturn(Right(()))

    val proc = new ParticipantTopologyTerminateProcessing(
      DefaultTestIdentities.synchronizerId,
      testedProtocolVersion,
      recordOrderPublisher,
      store,
      initialRecordTime,
      DefaultTestIdentities.participant1,
      pauseSynchronizerIndexingDuringPartyReplication = false,
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

  private lazy val party1participant1 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Confirmation)),
    )
  )
  private lazy val party1participant1_2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Submission),
      ),
    )
  )
  private lazy val party1participant1_2_threshold2 = mkAdd(
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.two,
      Seq(
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant2, Submission),
      ),
    )
  )
  private def partyParticipant1(
      party: PartyId
  ): SignedTopologyTransaction[Replace, PartyToParticipant] = mkAdd(
    PartyToParticipant.tryCreate(
      party,
      PositiveInt.one,
      Seq(HostingParticipant(participant1, Confirmation)),
    )
  )

  def add(
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): FutureUnlessShutdown[Unit] =
    add(store, timestamp, timestamp, transactions)

  def add(
      store: TopologyStore[TopologyStoreId.SynchronizerStore],
      sequencedTimestamp: CantonTimestamp,
      effectiveTimestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- store.update(
        SequencedTime(sequencedTimestamp),
        EffectiveTime(effectiveTimestamp),
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
          _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts))
        } yield {
          verify(rop, times(1)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _) =>
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
          )
        } yield {
          verify(rop, times(1)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _) =>
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
          )
        } yield {
          verify(rop, times(1)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          val events = eventCaptor.getAllValues.asScala.flatMap(_(CantonTimestamp.MinValue))
          events.size shouldBe 1
          forAll(events) {
            case TopologyTransactionEffective(_, events, _, _) =>
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

    "no events if the party rights threshold change" in {
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
          )
        } yield {
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 0
        }
      }
    }

    "no events if no change" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) = mk()
        val (cts0, _) = timestampWithCounter(0)
        val (cts1, sc1) = timestampWithCounter(1)

        for {
          _ <- add(store, cts0, List(party1participant1_2))
          _ <- add(store, cts1, List.empty)
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
        } yield {
          val events = eventCaptor.getAllValues.asScala
          events.size shouldBe 0
        }
      }
    }

    "no event publication if below initialRecordTime" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, _, rop) = mk(initialRecordTime = CantonTimestamp.ofEpochSecond(11))
        val (cts, sc) = timestampWithCounter(10)
        val (cts1, sc1) = timestampWithCounter(11)
        val (cts2, sc2) = timestampWithCounter(12)

        val txs = List(party1participant1)

        for {
          _ <- add(store, cts, txs)
          _ <- add(store, cts1, txs)
          _ <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts))
          _ <- proc.terminate(sc1, SequencedTime(cts1), EffectiveTime(cts1))
          _ = {
            verify(rop, times(0)).scheduleFloatingEventPublication(
              any[CantonTimestamp],
              any[CantonTimestamp => Option[FloatingUpdate]],
            )(any[TraceContext])
          }
          _ <- proc.terminate(sc2, SequencedTime(cts2), EffectiveTime(cts2))
        } yield {
          verify(rop, times(0)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          succeed
        }
      }
    }

    "fail if scheduling an event is not possible due to record order is already passed it" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, _, rop) = mk()
        val (cts, sc) = timestampWithCounter(10)

        val txs = List(party1participant1)

        when(
          rop.scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
        )
          .thenReturn(Left(CantonTimestamp.ofEpochSecond(15)))

        for {
          _ <- add(store, cts, txs)
          err <- proc.terminate(sc, SequencedTime(cts), EffectiveTime(cts)).failed
        } yield {
          verify(rop, times(1)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          err.getMessage should include(
            "Cannot schedule topology event as record time is already at"
          )
        }
      }
    }

    "re-schedule recovery events correctly" in {
      loggerFactory.suppress(EventsEnabledSuppressionRule) {
        val (proc, store, eventCaptor, rop) =
          mk(initialRecordTime = CantonTimestamp.ofEpochSecond(20))
        val (cts, _) = timestampWithCounter(10)
        val (cts2, _) = timestampWithCounter(12)
        val (cts3, _) = timestampWithCounter(15)
        val (cts4, _) = timestampWithCounter(17)
        val (cts5, _) = timestampWithCounter(20)
        val (cts6, _) = timestampWithCounter(21)
        val (cts7, _) = timestampWithCounter(23)
        val (cts8, _) = timestampWithCounter(25)
        val (cts9, _) = timestampWithCounter(27)

        val parties = List(1, 2, 3, 4, 5, 6, 7, 8).map(i =>
          PartyId(UniqueIdentifier.tryCreate(s"p$i", namespace))
        )
        val traceContexts = List.fill(8)(TraceContext.createNew())
        traceContexts.toSet.size shouldBe 8
        val traceContextMap = List(cts, cts2, cts3, cts4, cts5, cts6, cts7, cts8)
          .zip(traceContexts)
          .toMap
        def traceContextLookup: CantonTimestamp => FutureUnlessShutdown[Option[TraceContext]] =
          t =>
            FutureUnlessShutdown.pure(
              traceContextMap.get(t)
            )
        def extractFromUpdates(
            updates: List[Update]
        ): List[(Set[LfPartyId], CantonTimestamp, TraceContext)] =
          updates.collect { case u: TopologyTransactionEffective =>
            (
              u.events.collect { case ptp: PartyToParticipantAuthorization =>
                ptp.party
              },
              u.recordTime,
              u.traceContext,
            )
          }

        when(
          rop.scheduleFloatingEventPublication(
            any[CantonTimestamp],
            eventCaptor.capture(),
          )(any[TraceContext])
        ).thenAnswer { (timestamp: CantonTimestamp, _: CantonTimestamp => Option[Update]) =>
          if (timestamp == CantonTimestamp.ofEpochSecond(20))
            Left(CantonTimestamp.ofEpochSecond(20))
          else
            Right(())
        }

        val immediateEventCaptor: ArgumentCaptor[CantonTimestamp => Option[FloatingUpdate]] =
          ArgumentCaptor.forClass(classOf[CantonTimestamp => Option[FloatingUpdate]])
        when(
          rop.scheduleFloatingEventPublicationImmediately(
            immediateEventCaptor.capture()
          )(any[TraceContext])
        )
          .thenReturn(CantonTimestamp.ofEpochSecond(20))
        for {
          _ <- add(store, cts, cts2, List(partyParticipant1(parties.head))) // before
          _ <- add(store, cts2, cts4, List(partyParticipant1(parties(1)))) // before
          _ <- add(
            store,
            cts3,
            cts5,
            List(partyParticipant1(parties(2))),
          ) // on (conditional schedule)
          _ <- add(store, cts4, cts6, List(partyParticipant1(parties(3)))) // schedule
          _ <- add(
            store,
            cts5,
            cts7,
            List(
              partyParticipant1(parties(4)),
              partyParticipant1(parties(5)),
            ),
          ) // schedule
          _ <- add(
            store,
            cts6,
            cts8,
            List(partyParticipant1(parties(6))),
          ) // future processing, no schedule
          _ <- add(
            store,
            cts7,
            cts9,
            List(partyParticipant1(parties(7))),
          ) // future processing, no schedule
          _ <- proc.scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime = true,
            traceContextLookup,
            2,
          )
          _ = {
            verify(rop, times(3)).scheduleFloatingEventPublication(
              any[CantonTimestamp],
              any[CantonTimestamp => Option[FloatingUpdate]],
            )(any[TraceContext])
            verify(rop, times(0)).scheduleFloatingEventPublicationImmediately(
              any[CantonTimestamp => Option[FloatingUpdate]]
            )(any[TraceContext])
            extractFromUpdates(
              eventCaptor.getAllValues.asScala.toList
                .flatMap(Option(_)) // due to the two matchers, there could be nulls here
                .flatMap(_(CantonTimestamp.MinValue))
            ).toSet shouldBe Set(
              (Set(parties(2).toLf), cts5, traceContextMap(cts3)),
              (Set(parties(3).toLf), cts6, traceContextMap(cts4)),
              (Set(parties(4).toLf, parties(5).toLf), cts7, traceContextMap(cts5)),
            )
          }
          _ <- proc.scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime = false,
            traceContextLookup,
            2,
          )
        } yield {
          verify(rop, times(6)).scheduleFloatingEventPublication(
            any[CantonTimestamp],
            any[CantonTimestamp => Option[FloatingUpdate]],
          )(any[TraceContext])
          verify(rop, times(1)).scheduleFloatingEventPublicationImmediately(
            any[CantonTimestamp => Option[FloatingUpdate]]
          )(any[TraceContext])
          extractFromUpdates(
            immediateEventCaptor.getAllValues.asScala.toList
              .flatMap(_(CantonTimestamp.ofEpochSecond(20)))
          ).toSet shouldBe Set(
            (Set(parties(2).toLf), cts5, traceContextMap(cts3))
          )
        }
      }
    }
  }
}
