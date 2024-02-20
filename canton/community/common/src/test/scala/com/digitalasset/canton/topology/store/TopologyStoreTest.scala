// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.contravariantSemigroupal.*
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Replace}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.annotation.nowarn
import scala.concurrent.Future

trait TopologyStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterAll {

  def loggerFactory: NamedLoggerFactory

  protected implicit def traceContext: TraceContext

  val pid = ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::default"))
  lazy val submissionId = String255.tryCreate("submissionId")
  lazy val submissionId2 = String255.tryCreate("submissionId2")

  def partyMetadataStore(mk: () => PartyMetadataStore): Unit = {
    import DefaultTestIdentities.*
    "inserting new succeeds" in {
      val store = mk()
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          Some(participant1),
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        fetch <- store.metadataForParty(party1)
      } yield {
        fetch shouldBe Some(
          PartyMetadata(party1, None, Some(participant1))(CantonTimestamp.Epoch, submissionId)
        )
      }
    }

    "updating existing succeeds" in {
      val store = mk()
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          None,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party2,
          None,
          None,
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party1,
          Some(participant1),
          Some(String255.tryCreate("MoreName")),
          CantonTimestamp.Epoch,
          submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          party2,
          Some(participant3),
          Some(String255.tryCreate("Boooh")),
          CantonTimestamp.Epoch,
          submissionId,
        )
        meta1 <- store.metadataForParty(party1)
        meta2 <- store.metadataForParty(party2)
      } yield {
        meta1 shouldBe Some(
          PartyMetadata(party1, Some(String255.tryCreate("MoreName")), Some(participant1))(
            CantonTimestamp.Epoch,
            String255.empty,
          )
        )
        meta2 shouldBe Some(
          PartyMetadata(party2, Some(String255.tryCreate("Boooh")), Some(participant3))(
            CantonTimestamp.Epoch,
            String255.empty,
          )
        )
      }
    }

    "deal with delayed notifications" in {
      val store = mk()
      val rec1 =
        PartyMetadata(party1, None, Some(participant1))(CantonTimestamp.Epoch, submissionId)
      val rec2 =
        PartyMetadata(party2, Some(String255.tryCreate("Boooh")), Some(participant3))(
          CantonTimestamp.Epoch,
          submissionId2,
        )
      for {
        _ <- store.insertOrUpdatePartyMetadata(
          rec1.partyId,
          rec1.participantId,
          rec1.displayName,
          rec1.effectiveTimestamp,
          rec1.submissionId,
        )
        _ <- store.insertOrUpdatePartyMetadata(
          rec2.partyId,
          rec2.participantId,
          rec2.displayName,
          rec2.effectiveTimestamp,
          rec2.submissionId,
        )
        _ <- store.markNotified(rec2)
        notNotified <- store.fetchNotNotified()
      } yield {
        notNotified shouldBe Seq(rec1)
      }
    }

  }

  def topologyStore(mk: () => TopologyStore[TopologyStoreId]): Unit = {

    import DefaultTestIdentities.*
    val factory: TestingOwnerWithKeys =
      new TestingOwnerWithKeys(domainManager, loggerFactory, executorService)
    import factory.TestingTransactions.*

    implicit val toValidated
        : SignedTopologyTransaction[TopologyChangeOp] => ValidatedTopologyTransaction =
      x => ValidatedTopologyTransaction(x, None)

    def findTransactionsForTesting(
        store: TopologyStore[TopologyStoreId],
        from: CantonTimestamp,
        until: Option[CantonTimestamp],
        op: TopologyChangeOp,
    ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp]]] = {
      store
        .allTransactions()
        .map(
          _.result
            .filter(x =>
              x.validFrom.value == from && x.validUntil
                .map(_.value) == until && x.transaction.operation == op
            )
            .map(_.transaction)
        )
    }

    def getTransactions[Op <: TopologyChangeOp](validatedTx: List[ValidatedTopologyTransaction])(
        implicit checker: TopologyChangeOp.OpTypeChecker[Op]
    ): Seq[SignedTopologyTransaction[Op]] =
      SignedTopologyTransactions(validatedTx.map(_.transaction)).collectOfType[Op].result

    def findPositiveTx(
        store: TopologyStore[TopologyStoreId],
        ts: CantonTimestamp,
        types: Seq[DomainTopologyTransactionType],
        uidsO: Option[Seq[UniqueIdentifier]],
        nsO: Option[Seq[Namespace]],
    ) =
      store.findPositiveTransactions(
        asOf = ts,
        asOfInclusive = false,
        includeSecondary = false,
        types = types,
        filterUid = uidsO,
        filterNamespace = nsO,
      )

    def checkAsOf(
        fetch: (
            CantonTimestamp,
            Boolean,
        ) => Future[List[TopologyStateElement[TopologyMapping]]]
    ): Future[Assertion] = {
      def fetchAll(timestamp: CantonTimestamp, asOfInclusive: Boolean) =
        for {
          fst <- fetch(timestamp.immediatePredecessor, asOfInclusive)
          snd <- fetch(timestamp, asOfInclusive)
          trd <- fetch(timestamp.immediateSuccessor, asOfInclusive)
        } yield (fst, snd, trd)

      def checkR[T](res: (List[T], List[T], List[T]), hasData: (Boolean, Boolean, Boolean)) = {
        res._1.nonEmpty shouldBe hasData._1
        res._2.nonEmpty shouldBe hasData._2
        res._3.nonEmpty shouldBe hasData._3
      }

      for {
        startI <- fetchAll(ts, asOfInclusive = true)
        endI <- fetchAll(ts1, asOfInclusive = true)
        startE <- fetchAll(ts, asOfInclusive = false)
        endE <- fetchAll(ts1, asOfInclusive = false)
      } yield {
        checkR(startI, (false, true, true))
        checkR(endI, (true, false, false))
        checkR(startE, (false, false, true))
        checkR(endE, (true, true, false))
      }
    }

    @nowarn("msg=match may not be exhaustive")
    class SecondaryQueryTestDataSet {
      val partyId = PartyId(uid)
      val uid2 = UniqueIdentifier.tryFromProtoPrimitive("some::other")
      val participantId = ParticipantId(uid2)
      val transactions =
        List[RequestSide](RequestSide.From, RequestSide.To, RequestSide.Both).map(side =>
          factory.mkAdd(
            PartyToParticipant(side, partyId, participantId, ParticipantPermission.Submission)
          )
        )
      val _ :: trTo :: trB :: Nil = transactions

      def check(
          fetch: (
              Option[Seq[UniqueIdentifier]],
              Option[Seq[Namespace]],
              Boolean,
          ) => Future[List[TopologyStateElement[TopologyMapping]]]
      ): Future[Assertion] = {

        for {
          empty1 <- fetch(Some(Seq()), Some(Seq()), true)
          asUid <- fetch(Some(Seq(uid)), None, true)
          empty2 <- fetch(Some(Seq(uid2)), None, false)
          asUid2 <- fetch(Some(Seq(uid2)), None, true)
        } yield {
          empty1 shouldBe empty
          empty2 shouldBe empty
          asUid shouldBe transactions.map(_.transaction.element)
          asUid2 shouldBe List(trTo, trB).map(_.transaction.element)
        }

      }
    }

    def append(
        store: TopologyStore[TopologyStoreId],
        ts: CantonTimestamp,
        items: List[ValidatedTopologyTransaction],
    ): Future[Unit] = {
      store.append(SequencedTime(ts), EffectiveTime(ts), items)
    }

    def updateState(
        store: TopologyStore[TopologyStoreId],
        ts: CantonTimestamp,
        deactivate: Seq[UniquePath],
        positive: Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]],
    ): Future[Unit] = {
      store.updateState(SequencedTime(ts), EffectiveTime(ts), deactivate, positive)
    }

    "topology store" should {

      "deal with authorized" when {

        "don't panic on simple operations" in {
          val store = mk()
          for {
            _ <- append(store, ts, List())
            _ <- append(store, ts.plusSeconds(1), List(okm1))
            _ <- append(store, ts.plusSeconds(2), List(rokm1))
            _ <- append(store, ts.plusSeconds(3), List(ps1))
            _ <- append(store, ts.plusSeconds(4), List(ps2))
          } yield {
            assert(true)
          }
        }

        "bootstrap correctly updates timestamp" in {
          val store = mk()
          def gen(ts: CantonTimestamp, tx: SignedTopologyTransaction[TopologyChangeOp.Positive]) =
            StoredTopologyTransaction(SequencedTime(ts), EffectiveTime(ts), None, tx)
          for {
            _ <- store.bootstrap(
              StoredTopologyTransactions(
                Seq(
                  gen(ts.plusSeconds(1), okm1),
                  gen(ts.plusSeconds(2), ps1),
                  gen(ts.plusSeconds(3), ps2),
                  gen(ts.plusSeconds(4), ps3),
                  gen(ts.plusSeconds(5), p2p1),
                )
              )
            )
            currentTs <- store.timestamp()
          } yield currentTs.map(_._2.value) shouldBe Some(ts.plusSeconds(5))
        }

        "successfully append new items" in {
          val store = mk()
          for {
            _ <- append(store, ts, List(ns1k1, okm1, okm2, ps1, dpc1, dpc2))
            adds <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
            rems <- findTransactionsForTesting(store, ts, Some(ts), TopologyChangeOp.Remove)
            replaces <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Replace)
          } yield {
            adds should have length (4)
            rems should have length (0)
            replaces should have length (2)
          }
        }

        "deal with transient appends" in {
          val store = mk()
          for {
            _ <- append(store, ts, List(ns1k1, okm1, rokm1, ps1, rps1, ps2))
            transientAdd <- findTransactionsForTesting(store, ts, Some(ts), TopologyChangeOp.Add)
            added <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
            revocations <- findTransactionsForTesting(store, ts, Some(ts), TopologyChangeOp.Remove)
            replaces <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Replace)
          } yield {
            transientAdd shouldBe Seq(okm1, ps1)
            added shouldBe Seq(ns1k1, ps2)
            revocations shouldBe Seq(rokm1, rps1)
            replaces shouldBe Nil
          }
        }

        "deal with subsequent appends" in {
          val store = mk()

          for {
            _ <- append(store, ts, List(ns1k1, okm1, ps1))
            _ <- append(store, ts1, List(rokm1, rps1, ps2, ps3))
            updated <- findTransactionsForTesting(store, ts, Some(ts1), TopologyChangeOp.Add)
            addedPrev <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
            added <- findTransactionsForTesting(store, ts1, None, TopologyChangeOp.Add)
          } yield {
            updated shouldBe Seq(okm1, ps1)
            added shouldBe Seq(ps2, ps3)
            addedPrev shouldBe Seq(ns1k1)
          }
        }

        "sanely deal with duplicate adds in case of a faulty domain topology manager" in {
          val store = mk()
          for {
            _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
              append(store, ts, List(ns1k1, ns1k1, ns1k1, okm1, okm1, okm1, ps1, ps1, rps1, ps2)),
              seq => {
                seq.foreach(x => x.warningMessage should include("Discarding duplicate Add"))
                seq should have length (5)
              },
            )
            added <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
          } yield {
            added.toSet shouldBe Set(ns1k1, okm1, ps2)
          }
        }

        "sanely deal with duplicate replaces in case of a faulty domain topology manager" in {
          val store = mk()
          for {
            _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
              append(store, ts, List(ns1k1, dpc1, dpc1Updated)),
              seq => {
                seq.foreach(x => x.warningMessage should include("Discarding duplicate Replace"))
                seq should have length (1)
              },
            )
            added <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
            replaced <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Replace)
          } yield {
            added.toSet shouldBe Set(ns1k1)
            replaced.toSet shouldBe Set(dpc1Updated)
          }
        }

        "be idempotent" in {
          val store = mk()

          val first = List[ValidatedTopologyTransaction](ns1k1, okm1, ps1, dpc1)
          val snd = List[ValidatedTopologyTransaction](rokm1, ps2, rps1, dpc1Updated)

          for {
            _ <- append(store, ts, first)
            _ <- append(store, ts, first)
            initialAdds <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
            initialReplaces <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Replace)
            _ <- append(store, ts1, snd)
            _ <- append(store, ts1, snd)
            _ <- append(store, ts, first)
            _ <- append(store, ts1, snd)
            expiredAdds <- findTransactionsForTesting(store, ts, Some(ts1), TopologyChangeOp.Add)
            expiredReplaces <- findTransactionsForTesting(
              store,
              ts,
              Some(ts1),
              TopologyChangeOp.Replace,
            )
            currentAdds <- findTransactionsForTesting(store, ts1, None, TopologyChangeOp.Add)
            currentReplaces <- findTransactionsForTesting(
              store,
              ts1,
              None,
              TopologyChangeOp.Replace,
            )
            stillValid <- findTransactionsForTesting(store, ts, None, TopologyChangeOp.Add)
          } yield {
            initialAdds shouldBe getTransactions[TopologyChangeOp.Add](first)
            initialReplaces shouldBe getTransactions[TopologyChangeOp.Replace](first)

            expiredAdds shouldBe Seq(okm1, ps1)
            expiredReplaces shouldBe Seq(dpc1)

            currentAdds shouldBe Seq(ps2)
            stillValid shouldBe Seq(ns1k1)

            currentReplaces shouldBe Seq(dpc1Updated)
          }
        }

        "queries don't bail on empty table" in {
          val store = mk()
          for {
            _ <- store.timestamp()
            _ <- store.allTransactions()
            _ <- store.headTransactions
            _ <- store.findPositiveTransactions(
              CantonTimestamp.now(),
              true,
              false,
              DomainTopologyTransactionType.all,
              None,
              None,
            )
            _ <- store.inspect(
              true,
              timeQuery = TimeQuery.HeadState,
              None,
              None,
              None,
              "",
              false,
            )
            _ <- store.findPositiveTransactionsForMapping(okm1.transaction.element.mapping)
            _ <- store.findRemovalTransactionForMappings(Set(okm1.transaction.element))
            _ <- store.findDispatchingTransactionsAfter(ts1)
            _ <- store.findUpcomingEffectiveChanges(ts1)
            _ <- store.findTsOfParticipantStateChangesBefore(ts1, participant1, 100)
            _ <- store.findTransactionsInRange(ts, ts1)
          } yield {
            assert(true)
          }
        }

        "queries don't bail on empty time filtering" in {
          val store = mk()
          for {
            _ <- store.inspect(true, TimeQuery.Range(None, None), None, None, None, "", false)
          } yield {
            assert(true)
          }
        }

        "querying works as expected" in {
          val store = mk()
          val first = List[ValidatedTopologyTransaction](ns1k1, ns1k2, id1k1, okm1, ps1, dpc1)
          val snd =
            List[ValidatedTopologyTransaction](
              rokm1,
              okm2,
              ValidatedTopologyTransaction(id2k2, Some(TopologyTransactionRejection.NotAuthorized)),
            )

          // (Adds, Replaces)
          def snapshot(ts: CantonTimestamp) = {
            def inspectFor(op: TopologyChangeOp) = store
              .inspect(
                false,
                timeQuery = TimeQuery.Snapshot(ts),
                recentTimestampO = None,
                ops = Some(op),
                None,
                "",
                false,
              )
              .map(_.result.map(_.transaction))

            (inspectFor(TopologyChangeOp.Add), inspectFor(TopologyChangeOp.Replace)).mapN((_, _))
          }

          def testQueriesUsedForDomainTopologyDispatching(): Future[Unit] = for {
            empty1 <- store.findTsOfParticipantStateChangesBefore(ts, participant1, 100)
            haveP1 <- store.findTsOfParticipantStateChangesBefore(ts1, participant1, 100)
            empty2 <- store.findTsOfParticipantStateChangesBefore(ts1, participant2, 100)
            empty3 <- store.findTransactionsInRange(ts.minusSeconds(1), ts)
            firstQ <- store.findTransactionsInRange(ts.minusSeconds(1), ts.immediateSuccessor)
            empty4 <- store.findTransactionsInRange(ts, ts1)
            sndQ <- store.findTransactionsInRange(ts, ts1.immediateSuccessor)
          } yield {
            empty1 shouldBe empty
            empty2 shouldBe empty
            haveP1 shouldBe Seq(ts)
            empty3.result shouldBe empty
            empty4.result shouldBe empty
            def mapping(x: SignedTopologyTransaction[TopologyChangeOp]): TopologyMapping =
              x.transaction.element.mapping
            first.map(x => mapping(x.transaction)) shouldBe firstQ.result.map(x =>
              mapping(x.transaction)
            )
            snd
              .filter(_.rejectionReason.isEmpty)
              .map(x => mapping(x.transaction)) shouldBe sndQ.result.map(x =>
              mapping(x.transaction)
            )
          }

          for {
            _ <- append(store, ts, first)
            _ <- append(store, ts1, snd)
            maxTimestamp <- store.timestamp()
            all <- store.allTransactions()
            headState <- store.headTransactions
            empty1 <- snapshot(ts.immediatePredecessor)
            snapshot1 <- snapshot(ts.plusMillis(5))
            snapshot1b <- snapshot(ts1.immediatePredecessor)
            snapshot2 <- snapshot(ts1.plusMillis(5))
            empty2 <- store.findPositiveTransactionsForMapping(okm1.transaction.element.mapping)
            activeOkm2 <- store.findPositiveTransactionsForMapping(okm2.transaction.element.mapping)
            foundRokm1 <- store.findRemovalTransactionForMappings(Set(okm1.transaction.element))
            _ <- testQueriesUsedForDomainTopologyDispatching()
          } yield {
            maxTimestamp.map(_._2.value) should contain(ts1)
            all.result should have length ((first.length + snd.length - 1).toLong)
            headState.result.map(_.transaction) shouldBe Seq(ns1k1, ns1k2, id1k1, ps1, dpc1, okm2)
            empty1 shouldBe (Nil, Nil)
            empty2 shouldBe empty
            snapshot1 shouldBe (getTransactions[Add](first), getTransactions[Replace](first))
            snapshot1b shouldBe (getTransactions[Add](first), getTransactions[Replace](first))
            snapshot2 shouldBe (Seq(ns1k1, ns1k2, id1k1, ps1, okm2), Seq(dpc1))
            activeOkm2 should contain(okm2)
            foundRokm1 shouldBe Seq(rokm1)
          }
        }

        "test bootstrapping queries" in {
          val store = mk()

          // we have the following changes
          val changes = List(
            (0, 0) -> Seq(ns1k1),
            (100, 100) -> Seq(ns1k2, dpc1), // epsilon 250ms
            (110, 260) -> Seq(id1k1),
            (120, 270) -> Seq(dpc1Updated),
            (150, 300) -> Seq(okm1),
          )

          implicit class AddMs(ts: CantonTimestamp) {
            def +(ms: Int): CantonTimestamp = ts.plusMillis(ms.toLong)
          }

          val storeF = MonadUtil.sequentialTraverse_(changes) {
            case ((sequenced, effective), items) =>
              store.append(
                SequencedTime(ts + sequenced),
                EffectiveTime(ts + effective),
                items.map(toValidated),
              )
          }

          def change(sequenced: Int, effective: Int, dpc: Option[Int]): TopologyStore.Change = {
            dpc.fold(
              TopologyStore.Change
                .Other(
                  SequencedTime(ts + sequenced),
                  EffectiveTime(ts + effective),
                ): TopologyStore.Change
            ) { epsilon =>
              TopologyStore.Change.TopologyDelay(
                SequencedTime(ts + sequenced),
                EffectiveTime(ts + effective),
                NonNegativeFiniteDuration.tryOfMillis(epsilon.toLong),
              )
            }
          }

          for {
            _ <- storeF
            empty1 <- store.findUpcomingEffectiveChanges(ts + 301)
            last1 <- store.findUpcomingEffectiveChanges(ts + 271)
            pendingDpc <- store.findUpcomingEffectiveChanges(ts + 270)
            firstDpc <- store.findUpcomingEffectiveChanges(ts + 101)
            timestamps <- store.timestamp(useStateStore = false)
          } yield {
            val change150 = change(150, 300, None)
            val change120 = change(120, 270, Some(100))
            val change110 = change(110, 260, None)
            empty1 shouldBe empty
            last1 shouldBe Seq(change150)
            pendingDpc shouldBe Seq(change120, change150)
            firstDpc shouldBe Seq(change110, change120, change150)
            timestamps should contain((SequencedTime(ts + 150), EffectiveTime(ts + 300)))
          }

        }

        "namespace filter behaves correctly" in {
          val store = mk()
          import factory.SigningKeys.*
          val ns2k3 =
            factory.mkAdd(
              NamespaceDelegation(Namespace(key2.fingerprint), key2, isRootDelegation = true),
              key2,
            )
          val ns3k3 =
            factory.mkAdd(
              NamespaceDelegation(Namespace(key3.fingerprint), key3, isRootDelegation = true),
              key3,
            )
          val uid1 = UniqueIdentifier.tryCreate("one", key1.fingerprint.unwrap)
          val uid2 = UniqueIdentifier.tryCreate("two", key2.fingerprint.unwrap)
          val uid3 = UniqueIdentifier.tryCreate("three", key3.fingerprint.unwrap)
          val ok1k1 = factory.mkAdd(OwnerToKeyMapping(ParticipantId(uid1), key4), key1)
          val ok2k2 = factory.mkAdd(OwnerToKeyMapping(ParticipantId(uid2), key5), key2)
          val ok3k3 = factory.mkAdd(OwnerToKeyMapping(ParticipantId(uid3), key6), key3)
          val first = List[ValidatedTopologyTransaction](ns2k3, ns3k3, ok1k1, ok2k2, ok3k3, id1k1)

          def getMappings(txs: PositiveStoredTopologyTransactions): Set[TopologyMapping] =
            txs.combine.result.map(_.transaction.transaction.element.mapping).toSet

          for {
            _ <- append(store, ts, first)
            nsQ <- store.findPositiveTransactions(
              ts1,
              asOfInclusive = false,
              includeSecondary = false,
              types = Seq(
                DomainTopologyTransactionType.OwnerToKeyMapping,
                DomainTopologyTransactionType.NamespaceDelegation,
              ),
              filterUid = None,
              filterNamespace = Some(Seq(uid2.namespace)),
            )
            uidQ <- store.findPositiveTransactions(
              ts1,
              asOfInclusive = false,
              includeSecondary = false,
              types = Seq(
                DomainTopologyTransactionType.OwnerToKeyMapping,
                DomainTopologyTransactionType.NamespaceDelegation,
              ),
              filterUid = Some(Seq(uid3)),
              filterNamespace = Some(Seq(uid2.namespace)),
            )
          } yield {
            getMappings(nsQ) shouldBe Set(ns2k3, ok2k2).map(_.transaction.element.mapping)

            getMappings(uidQ) shouldBe Set(
              ns2k3,
              ok2k2,
              ok3k3,
            ).map(_.transaction.element.mapping)
          }
        }

        "state query works correctly" in {
          val store = mk()
          val first = List[ValidatedTopologyTransaction](ns1k1, ns1k2, id1k1, okm1, ps1, dpc1)
          val snd =
            List[ValidatedTopologyTransaction](
              rps1,
              ps2,
              ps3,
              rokm1,
              okm2,
              ValidatedTopologyTransaction(id2k2, Some(TopologyTransactionRejection.NotAuthorized)),
            )

          def findPositiveTopologyStateElements(
              ts: CantonTimestamp,
              types: Seq[DomainTopologyTransactionType],
              uidsO: Option[Seq[UniqueIdentifier]] = None,
          ) = findPositiveTx(store, ts, types, uidsO, None).map(_.toIdentityState)

          for {
            _ <- append(store, ts, first)
            _ <- append(store, ts1, snd)

            empty1 <- findPositiveTopologyStateElements(
              ts,
              Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
            )
            allUids <- findPositiveTopologyStateElements(
              ts1.immediateSuccessor,
              Seq(DomainTopologyTransactionType.ParticipantState),
            )
            uidFilter <- findPositiveTopologyStateElements(
              ts1.immediateSuccessor,
              Seq(DomainTopologyTransactionType.ParticipantState),
              Some(Seq(ps1m.participant.uid)),
            )
            prevPs <- findPositiveTopologyStateElements(
              ts.immediateSuccessor,
              Seq(DomainTopologyTransactionType.ParticipantState),
            )
            findOkm1 <- findPositiveTopologyStateElements(
              ts.immediateSuccessor,
              Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
            )
            findOkm2 <- findPositiveTopologyStateElements(
              ts1.immediateSuccessor,
              Seq(DomainTopologyTransactionType.OwnerToKeyMapping),
            )
            findDpc1 <- findPositiveTopologyStateElements(
              ts.immediateSuccessor,
              Seq(DomainTopologyTransactionType.DomainParameters),
              uidsO = Some(Seq(uid)),
            )
            emptyFindDpc1 <- findPositiveTopologyStateElements(
              ts.immediateSuccessor,
              Seq(DomainTopologyTransactionType.DomainParameters),
              uidsO = Some(Seq(UniqueIdentifier(Identifier.tryCreate("myOtherId"), namespace))),
            )
          } yield {
            empty1 shouldBe empty
            allUids.toSet shouldBe Seq(ps2, ps3).map(_.transaction.element).toSet
            uidFilter shouldBe Seq(ps2.transaction.element)
            prevPs shouldBe Seq(ps1.transaction.element)
            findOkm1 shouldBe Seq(okm1.transaction.element)
            findOkm2 shouldBe Seq(okm2.transaction.element)
            findDpc1 shouldBe Seq(dpc1.transaction.element)
            emptyFindDpc1 shouldBe empty
          }

        }

        "state query combines uid and ns queries correctly" in {
          val store = mk()
          import factory.SigningKeys.*
          val ns1 = Namespace(key1.fingerprint)
          val ns2 = Namespace(key2.fingerprint)
          val ns3 = Namespace(key3.fingerprint)
          val ns4 = Namespace(key4.fingerprint)

          def pid(namespace: Namespace) =
            ParticipantId(UniqueIdentifier(Identifier.tryCreate("participant"), namespace))

          val okm1 = factory.mkAdd(OwnerToKeyMapping(pid(ns1), key1))
          val okm2 = factory.mkAdd(OwnerToKeyMapping(pid(ns2), key2))
          val okm3 = factory.mkAdd(OwnerToKeyMapping(pid(ns3), key3))

          val trans = List[ValidatedTopologyTransaction](okm1, okm2, okm3)
          val tp = Seq(DomainTopologyTransactionType.OwnerToKeyMapping)
          for {
            _ <- append(store, ts, trans)
            empty1 <- findPositiveTx(store, ts1, tp, Some(Seq(pid(ns4).uid)), None)
              .map(_.toIdentityState)
            all <- findPositiveTx(store, ts1, tp, None, None).map(_.toIdentityState)
            p1AndP2 <- findPositiveTx(store, ts1, tp, Some(Seq(pid(ns2).uid)), Some(Seq(ns1)))
              .map(_.toIdentityState)
            p3 <- findPositiveTx(store, ts1, tp, None, Some(Seq(ns3))).map(_.toIdentityState)
          } yield {
            empty1 shouldBe empty
            all shouldBe Seq(okm1, okm2, okm3).map(_.transaction.element)
            p1AndP2 shouldBe Seq(okm1, okm2).map(_.transaction.element)
            p3 shouldBe Seq(okm3.transaction.element)
          }
        }

        "finding initial state succeeds" in {
          val store = mk()

          val first = List[ValidatedTopologyTransaction](
            factory.mkAdd(OwnerToKeyMapping(domainManager, factory.SigningKeys.key1)),
            factory.mkAdd(OwnerToKeyMapping(mediator, factory.SigningKeys.key2)),
          )
          val snd = List[ValidatedTopologyTransaction](
            factory.mkAdd(OwnerToKeyMapping(ParticipantId(uid), factory.SigningKeys.key7)),
            factory.mkAdd(OwnerToKeyMapping(sequencerId, factory.SigningKeys.key3)),
            factory.mkAdd(OwnerToKeyMapping(sequencerId, factory.SigningKeys.key4)),
            factory.mkAdd(OwnerToKeyMapping(ParticipantId(uid2), factory.SigningKeys.key5)),
            factory.mkAdd(OwnerToKeyMapping(sequencerId, factory.SigningKeys.key5)),
          )
          val dummy = (1 to 110)
            .map { x =>
              factory.mkAdd(
                NamespaceDelegation(Namespace(Fingerprint.tryCreate(s"test$x")), namespaceKey, true)
              )
            }
            .map(toValidated)
          for {
            _ <- append(store, ts, first)
            _ <- append(store, ts1, dummy.toList)
            _ <- append(store, ts1.plusSeconds(1), snd)
            initial <- store.findInitialState(domainManager)
          } yield {
            initial shouldBe Map[KeyOwner, Seq[PublicKey]](
              domainManager -> Seq(factory.SigningKeys.key1),
              sequencerId -> Seq(factory.SigningKeys.key3, factory.SigningKeys.key4),
            )
          }
        }

        "querying for participant initial state works" in {
          val store = mk()

          val namespace = factory.SigningKeys.key1
          val participant1 =
            ParticipantId(
              UniqueIdentifier(Identifier.tryCreate("foo"), Namespace(namespace.fingerprint))
            )
          val okmS = factory.mkAdd(OwnerToKeyMapping(participant1, factory.SigningKeys.key7))
          val okmE = factory.mkAdd(OwnerToKeyMapping(participant1, factory.EncryptionKeys.key1))
          val crtE = factory.mkAdd(
            ParticipantState(
              RequestSide.To,
              domainId,
              participant1,
              ParticipantPermission.Submission,
              TrustLevel.Ordinary,
            )
          )

          val first = List[ValidatedTopologyTransaction](ns1k1, p2p1, p2p2, okmS, okmE, crtE)
          for {
            _ <- append(store, ts, first)
            bootstrap <- store
              .findParticipantOnboardingTransactions(participant1, domainId)
              .failOnShutdown
            empty1 <- store
              .findParticipantOnboardingTransactions(participant2, domainId)
              .failOnShutdown
          } yield {
            empty1 shouldBe empty
            bootstrap shouldBe List(ns1k1, okmS, okmE, crtE)
          }
        }

        "asOf inclusive flag works" in {
          val store = mk()
          val first = List[ValidatedTopologyTransaction](ns1k1)
          def fetch(asOf: CantonTimestamp, asOfInclusive: Boolean) =
            store
              .findPositiveTransactions(
                asOf,
                asOfInclusive,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
          for {
            _ <- append(store, ts, first)
            _ <- append(store, ts1, List[ValidatedTopologyTransaction](factory.revert(ns1k1)))
            res <- checkAsOf(fetch)
          } yield res
        }

        "secondary uid/ns fetch works" in {
          val tmp = new SecondaryQueryTestDataSet()
          val store = mk()

          def fetch(
              uids: Option[Seq[UniqueIdentifier]],
              ns: Option[Seq[Namespace]],
              includeSecondary: Boolean,
          ) =
            store
              .findPositiveTransactions(
                ts,
                asOfInclusive = true,
                includeSecondary,
                DomainTopologyTransactionType.all,
                uids,
                ns,
              )
              .map(_.toIdentityState)
          for {
            _ <- append(store, ts, tmp.transactions.map(toValidated))
            res <- tmp.check(fetch)
          } yield {
            res
          }
        }

        "rejected txs" when {
          "insert rejected positive transactions correctly" in {
            val store = mk()

            val unauthorizedTransactions = List(
              ValidatedTopologyTransaction(
                ns1k1,
                Some(TopologyTransactionRejection.NotAuthorized),
              ),
              ValidatedTopologyTransaction(
                dpc1,
                Some(TopologyTransactionRejection.NotAuthorized),
              ),
            )

            for {
              _ <- append(store, ts, unauthorizedTransactions)
              flt <- store.findPositiveTransactions(
                ts,
                asOfInclusive = true,
                false,
                types = DomainTopologyTransactionType.all,
                None,
                None,
              )
              all <- store.allTransactions()
            } yield {
              flt.combine.result shouldBe empty
              all.result shouldBe empty
            }
          }

          "insert removes correctly" in {
            val store = mk()
            val unauthorizedRemoves = List(
              ValidatedTopologyTransaction(
                factory.revert(ns1k1),
                Some(TopologyTransactionRejection.NotAuthorized),
              )
            )

            for {
              _ <- append(store, ts, List(ValidatedTopologyTransaction(ns1k1, None)))
              _ <- append(store, ts1, unauthorizedRemoves)
              flt <- store.findPositiveTransactions(
                ts1,
                asOfInclusive = true,
                false,
                types = DomainTopologyTransactionType.all,
                None,
                None,
              )
              all <- store.allTransactions()
            } yield {
              assert(flt.combine.result.nonEmpty, flt.combine.result)
              assert(all.result.nonEmpty, all.result)
            }
          }
        }
      }

      "deal with active state" when {

        "support insertions and updating" in {
          val store = mk()
          for {
            // empty change
            _ <- updateState(store, ts, deactivate = Seq(), positive = Seq())
            // add a few items
            _ <- updateState(
              store,
              ts,
              deactivate = Seq(),
              positive = Seq(ns1k1, id2k2, okm1, dpc1),
            )
            // should not mix with normal state txs
            empty1 <- store
              .findPositiveTransactions(
                ts,
                true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
            // should find items
            items1 <- store
              .findStateTransactions(
                ts,
                asOfInclusive = true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
            // update and add
            _ <- updateState(
              store,
              ts1,
              deactivate = Seq(id2k2.uniquePath, dpc1.uniquePath),
              positive = Seq(okm2, dpc1Updated),
            )
            // repeat previous query
            items2 <- store
              .findStateTransactions(
                ts,
                asOfInclusive = true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
            // check new state
            items3 <- store
              .findStateTransactions(
                ts1,
                asOfInclusive = true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
          } yield {
            empty1 shouldBe empty
            items1 shouldBe List(ns1k1, id2k2, okm1, dpc1).map(_.transaction.element)
            items1 shouldBe items2
            items3 shouldBe List(ns1k1, okm1, okm2, dpc1Updated).map(_.transaction.element)
          }
        }

        "should be idempotent" in {
          val store = mk()
          def fetch() =
            store
              .findStateTransactions(
                ts1,
                asOfInclusive = true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
          for {
            _ <- updateState(
              store,
              ts,
              deactivate = Seq(),
              positive = Seq(ns1k1, id2k2, okm1, dpc1),
            )
            _ <- updateState(store, ts1, deactivate = Seq(id2k2.uniquePath), positive = Seq(okm2))
            st1 <- fetch()
            _ <- updateState(
              store,
              ts,
              deactivate = Seq(),
              positive = Seq(ns1k1, id2k2, okm1, dpc1),
            )
            st2 <- fetch()
            _ <- updateState(store, ts1, deactivate = Seq(id2k2.uniquePath), positive = Seq(okm2))
            st3 <- fetch()
          } yield {
            st1 shouldBe List(ns1k1, okm1, okm2, dpc1).map(_.transaction.element)
            st1 shouldBe st2
            st1 shouldBe st3
          }
        }

        "filtering should work" in {
          val store = mk()
          val fakeUid = UniqueIdentifier.tryFromProtoPrimitive("not::valid")
          def fetch(uid: Option[Seq[UniqueIdentifier]], ns: Option[Seq[Namespace]]) =
            store
              .findStateTransactions(
                ts1,
                asOfInclusive = true,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                uid,
                ns,
              )
              .map(_.toIdentityState)
          for {
            _ <- updateState(
              store,
              ts,
              deactivate = Seq(),
              positive = Seq(ns1k1, id2k2, okm1, dpc1),
            )
            empty1 <- fetch(Some(Seq()), None)
            empty2 <- fetch(None, Some(Seq()))
            empty3 <- fetch(Some(Seq(fakeUid)), None)
            empty4 <- fetch(None, Some(Seq(fakeUid.namespace)))
            empty5 <- fetch(Some(Seq(fakeUid)), Some(Seq(fakeUid.namespace)))
            find1 <- fetch(Some(Seq(uid2)), None)
            find2 <- fetch(None, Some(Seq(uid2.namespace)))
          } yield {
            forAll(Seq(empty1, empty2, empty3, empty4, empty5)) {
              _ shouldBe empty
            }
            find1 shouldBe List(id2k2).map(_.transaction.element)
            find2 shouldBe List(id2k2, okm1, dpc1).map(_.transaction.element)
          }
        }

        "asOf filtering should work" in {
          val store = mk()
          def fetch(asOf: CantonTimestamp, asOfInclusive: Boolean) =
            store
              .findStateTransactions(
                asOf,
                asOfInclusive,
                includeSecondary = false,
                DomainTopologyTransactionType.all,
                None,
                None,
              )
              .map(_.toIdentityState)
          for {
            _ <- updateState(store, ts, deactivate = Seq(), positive = Seq(ns1k1))
            _ <- updateState(store, ts1, deactivate = Seq(ns1k1.uniquePath), positive = Seq())
            res <- checkAsOf(fetch)
          } yield res
        }

        "secondary uid/ns fetch works" in {
          val store = mk()
          val tmp = new SecondaryQueryTestDataSet
          import tmp.*

          def fetch(
              uids: Option[Seq[UniqueIdentifier]],
              ns: Option[Seq[Namespace]],
              includeSecondary: Boolean,
          ) =
            store
              .findStateTransactions(
                ts,
                asOfInclusive = true,
                includeSecondary,
                DomainTopologyTransactionType.all,
                uids,
                ns,
              )
              .map(_.toIdentityState)
          for {
            _ <- updateState(store, ts, deactivate = Seq(), positive = transactions)
            res <- tmp.check(fetch)
          } yield res
        }

        "inspection for parties" in {
          val store = mk()
          val ts = CantonTimestamp.Epoch
          for {
            _ <- updateState(store, ts, deactivate = Seq(), positive = List(ps1, p2p1, p2p2))
            res <- store.inspectKnownParties(ts.immediateSuccessor, "one", "", 100)
            res2 <- store.inspectKnownParties(ts.immediateSuccessor, "", "", 1)
            empty1 <- store.inspectKnownParties(ts.immediateSuccessor, "three", "", 100)
          } yield {
            empty1 shouldBe empty
            res.toSeq should have length (1)
            res2.toSeq should have length (1)
          }
        }

      }

      "using watermarks" when {
        "allows querying, insertion and updating" in {
          val store = mk()
          val ts = CantonTimestamp.Epoch
          for {
            emptyO <- store.currentDispatchingWatermark
            _ <- store.updateDispatchingWatermark(ts)
            resTs <- store.currentDispatchingWatermark
            _ <- store.updateDispatchingWatermark(ts.immediateSuccessor)
            resTs2 <- store.currentDispatchingWatermark
          } yield {
            emptyO shouldBe empty
            resTs should contain(ts)
            resTs2 should contain(ts.immediateSuccessor)
          }
        }

        "querying dispatching transactions works" in {
          val store = mk()
          val ts1 = ts.plusSeconds(1)
          def fetch(timestamp: CantonTimestamp) =
            store.findDispatchingTransactionsAfter(timestamp).map(_.toDomainTopologyTransactions)
          for {
            _ <- append(store, ts, List(ns1k1, okm1))
            state1 <- fetch(CantonTimestamp.MinValue)
            empty1 <- fetch(ts)
            _ <- append(store, ts1, List(rokm1, okm2))
            state2 <- fetch(CantonTimestamp.MinValue)
            sub2 <- fetch(ts)
            empty2 <- fetch(ts1)
          } yield {
            empty1 shouldBe empty
            empty2 shouldBe empty
            state1 shouldBe Seq(ns1k1, okm1)
            state2 shouldBe Seq(ns1k1, rokm1, okm2)
            sub2 shouldBe Seq(rokm1, okm2)
          }

        }

      }

      "store the same topology transaction for different protocol versions" in {
        val store = mk()

        val baseTx =
          NamespaceDelegation(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          )
        val stateUpdateId = TopologyElementId.generate()

        def addTx(protocolVersion: ProtocolVersion) =
          TopologyStateUpdate(
            TopologyChangeOp.Add,
            TopologyStateUpdateElement(stateUpdateId, baseTx),
            protocolVersion,
          )

        val oldTx = factory.mkTrans(addTx(ProtocolVersion.v5), namespaceKey)
        val newTx = factory.mkTrans(addTx(ProtocolVersion.v6), namespaceKey)

        for {
          _ <- append(store, ts, List(oldTx))
          _ <- append(store, ts.plusMillis(1), List(newTx))
          txs <- store.allTransactions()
        } yield {
          txs.result.size shouldEqual 2
        }

      }

    }
  }

}
