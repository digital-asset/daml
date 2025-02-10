// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{SigningKeyUsage, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.db.DbTopologyStoreHelper
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
trait StoreBasedTopologySnapshotTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown {

  import EffectiveTimeTestHelpers.*

  def topologySnapshot(mk: () => TopologyStore[TopologyStoreId]): Unit = {

    val factory = new TestingOwnerWithKeys(
      DefaultTestIdentities.participant1,
      loggerFactory,
      parallelExecutionContext,
    )
    import DefaultTestIdentities.*
    import factory.*
    import factory.TestingTransactions.*

    lazy val party1participant1 = mkAdd(
      PartyToParticipant.tryCreate(
        party1,
        PositiveInt.one,
        Seq(HostingParticipant(participant1, Confirmation)),
      )
    )
    lazy val party2participant1_2 = mkAdd(
      PartyToParticipant.tryCreate(
        party2,
        PositiveInt.one,
        Seq(
          HostingParticipant(participant1, Submission),
          HostingParticipant(participant2, Submission),
        ),
      )
    )

    class Fixture {
      val store: TopologyStore[TopologyStoreId] = mk()
      val client =
        new StoreBasedSynchronizerTopologyClient(
          mock[Clock],
          synchronizerId,
          store,
          StoreBasedSynchronizerTopologyClient.NoPackageDependencies,
          DefaultProcessingTimeouts.testing,
          FutureSupervisor.Noop,
          loggerFactory,
        )

      def add(
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
          _ <- client
            .observed(timestamp, timestamp, SequencerCounter(1), transactions)
        } yield ()

      def observed(ts: CantonTimestamp): Unit =
        observed(SequencedTime(ts), EffectiveTime(ts))

      def observed(st: SequencedTime, et: EffectiveTime): Unit =
        client
          .observed(st, et, SequencerCounter(0), List())
          .futureValueUS

      def updateHead(
          st: SequencedTime,
          et: EffectiveTime,
          at: ApproximateTime,
          potentialTopologyChange: Boolean,
      ): Unit =
        client
          .updateHead(st, et, at, potentialTopologyChange)
    }

    "waiting for snapshots" should {

      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)

      "announce snapshot if there is one" in {
        val fixture = new Fixture()
        import fixture.*
        observed(ts1)
        client.snapshotAvailable(ts1) shouldBe true
        client.snapshotAvailable(ts2) shouldBe false
        observed(ts2.immediatePredecessor)
        client.snapshotAvailable(ts2) shouldBe true
      }

      "correctly get notified" in {
        val fixture = new Fixture()
        import fixture.*
        val awaitTimestampF = client.awaitTimestamp(ts2).getOrElse(fail("expected future"))
        observed(ts1)
        awaitTimestampF.isCompleted shouldBe false
        observed(ts2.immediatePredecessor)
        awaitTimestampF.isCompleted shouldBe true
      }

      "just return None if snapshot already exists" in {
        val fixture = new Fixture()
        import fixture.*
        observed(ts1)
        val awaitTimestampF = client.awaitTimestamp(ts1)
        awaitTimestampF shouldBe None
      }
    }

    "waiting for sequenced time" should {
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(60)

      "correctly get notified on observed" in {
        val fixture = new Fixture()
        import fixture.*
        val awaitSequencedTimestampF =
          client.awaitSequencedTimestampUS(ts2).getOrElse(fail("expected future"))

        observed(SequencedTime(ts1), EffectiveTime(ts1))
        awaitSequencedTimestampF.isCompleted shouldBe false
        observed(SequencedTime(ts2.immediatePredecessor), EffectiveTime(ts1))
        awaitSequencedTimestampF.isCompleted shouldBe true
      }

      "correctly get notified on updateHead" in {
        Table("potential topology change", true, false).forEvery { potentialTopologyChange =>
          val fixture = new Fixture()
          import fixture.*
          val awaitSequencedTimestampF =
            client.awaitSequencedTimestampUS(ts2).getOrElse(fail("expected future"))

          updateHead(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            ApproximateTime(ts1),
            potentialTopologyChange,
          )
          awaitSequencedTimestampF.isCompleted shouldBe false
          updateHead(
            SequencedTime(ts2.immediatePredecessor),
            EffectiveTime(ts1),
            ApproximateTime(ts1),
            potentialTopologyChange,
          )
          awaitSequencedTimestampF.isCompleted shouldBe true
        }
      }

      "just return None if sequenced time already known" in {
        val fixture = new Fixture()
        import fixture.*
        observed(SequencedTime(ts1), EffectiveTime(CantonTimestamp.MinValue))
        client.awaitSequencedTimestampUS(ts1) shouldBe None
      }
    }

    "work with empty store" in {
      val fixture = new Fixture()
      import fixture.*
      val _ = client.currentSnapshotApproximation
      val mrt = client.approximateTimestamp
      val sp = client.trySnapshot(mrt)
      for {
        parties <- sp.activeParticipantsOf(party1.toLf)
        keys <- sp.signingKeys(participant1, SigningKeyUsage.All)
      } yield {
        parties shouldBe empty
        keys shouldBe empty
      }
    }

    def compareMappings(
        result: Map[ParticipantId, ParticipantAttributes],
        expected: Map[ParticipantId, ParticipantPermission],
    ) =
      result.map(x => (x._1, x._2.permission)) shouldBe expected

    def compareKeys(result: Seq[SigningPublicKey], expected: Seq[SigningPublicKey]) =
      result.map(_.fingerprint) shouldBe expected.map(_.fingerprint)

    "deliver correct results" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.add(
          ts,
          Seq(
            dpc1,
            p1_nsk2,
            p1_otk,
            p1_dtc,
            p2_nsk2,
            party1participant1,
            party2participant1_2,
          ),
        )
        _ = fixture.client.observed(
          ts.immediateSuccessor,
          ts.immediateSuccessor,
          SequencerCounter(0),
          Seq(),
        )
        recent = fixture.client.currentSnapshotApproximation
        party1Mappings <- recent.activeParticipantsOf(party1.toLf)
        party2Mappings <- recent.activeParticipantsOf(party2.toLf)
        keys <- recent.signingKeys(participant1, SigningKeyUsage.All)
      } yield {
        party1Mappings.keySet shouldBe Set(participant1)
        party1Mappings.get(participant1).map(_.permission) shouldBe Some(
          ParticipantPermission.Confirmation
        )
        party2Mappings.keySet shouldBe Set(participant1)
        party2Mappings.get(participant1).map(_.permission) shouldBe Some(
          ParticipantPermission.Submission
        )
        keys.map(_.id) shouldBe Seq(SigningKeys.key1.id)
      }
    }

    "properly deals with participants with lower synchronizer privileges" in {
      val fixture = new Fixture()
      for {
        _ <- fixture.add(ts, Seq(dpc1, p1_otk, p1_dtc, party1participant1, p1_pdp_observation))
        _ = fixture.client.observed(
          ts.immediateSuccessor,
          ts.immediateSuccessor,
          SequencerCounter(0),
          Seq(),
        )
        snapshot <- fixture.client.snapshot(ts.immediateSuccessor)
        party1Mappings <- snapshot.activeParticipantsOf(party1.toLf)
      } yield {
        compareMappings(party1Mappings, Map(participant1 -> ParticipantPermission.Observation))
      }
    }

    "work properly with updates" in {
      val fixture = new Fixture()
      val ts2 = ts1.plusSeconds(1)
      for {
        _ <- fixture.add(
          ts,
          Seq(
            seq_okm_k2,
            dpc1,
            p1_otk,
            p1_dtc,
            party1participant1,
            party2participant1_2,
          ),
        )
        _ <- fixture.add(
          ts1,
          Seq(
            mkRemoveTx(seq_okm_k2),
            med_okm_k3,
            p2_otk,
            p2_dtc,
            p1_pdp_observation,
            p2_pdp_confirmation,
          ),
        )
        _ <- fixture.add(ts2, Seq(mkRemoveTx(p1_pdp_observation), mkRemoveTx(p1_dtc)))
        _ = fixture.client.observed(
          ts2.immediateSuccessor,
          ts2.immediateSuccessor,
          SequencerCounter(0),
          Seq(),
        )
        snapshotA <- fixture.client.snapshot(ts1)
        snapshotB <- fixture.client.snapshot(ts1.immediateSuccessor)
        snapshotC <- fixture.client.snapshot(ts2.immediateSuccessor)
        party1Ma <- snapshotA.activeParticipantsOf(party1.toLf)
        party1Mb <- snapshotB.activeParticipantsOf(party1.toLf)
        party2Ma <- snapshotA.activeParticipantsOf(party2.toLf)
        party2Mb <- snapshotB.activeParticipantsOf(party2.toLf)
        party2Mc <- snapshotC.activeParticipantsOf(party2.toLf)
        keysMa <- snapshotA.signingKeys(mediatorId, SigningKeyUsage.All)
        keysMb <- snapshotB.signingKeys(mediatorId, SigningKeyUsage.All)
        keysSa <- snapshotA.signingKeys(sequencerId, SigningKeyUsage.All)
        keysSb <- snapshotB.signingKeys(sequencerId, SigningKeyUsage.All)
        partPermA <- snapshotA.findParticipantState(participant1)
        partPermB <- snapshotB.findParticipantState(participant1)
        partPermC <- snapshotC.findParticipantState(participant1)
        admin1a <- snapshotA.activeParticipantsOf(participant1.adminParty.toLf)
        admin1b <- snapshotB.activeParticipantsOf(participant1.adminParty.toLf)
      } yield {
        compareMappings(party1Ma, Map(participant1 -> ParticipantPermission.Confirmation))
        compareMappings(party1Mb, Map(participant1 -> ParticipantPermission.Observation))
        compareMappings(party2Ma, Map(participant1 -> ParticipantPermission.Submission))
        compareMappings(
          party2Mb,
          Map(
            participant1 -> ParticipantPermission.Observation,
            participant2 -> ParticipantPermission.Confirmation,
          ),
        )
        compareMappings(party2Mc, Map(participant2 -> ParticipantPermission.Confirmation))
        compareKeys(keysMa, Seq())
        compareKeys(keysMb, Seq(SigningKeys.key3))
        compareKeys(keysSa, Seq(SigningKeys.key2))
        compareKeys(keysSb, Seq())
        partPermA
          .valueOrFail("No permission for participant1 in snapshotA")
          .permission shouldBe ParticipantPermission.Submission
        partPermB
          .valueOrFail("No permission for participant1 in snapshotB")
          .permission shouldBe ParticipantPermission.Observation
        partPermC shouldBe None
        compareMappings(admin1a, Map(participant1 -> ParticipantPermission.Submission))
        compareMappings(admin1b, Map(participant1 -> ParticipantPermission.Observation))
      }
    }
  }
}

class StoreBasedTopologySnapshotTestInMemory extends StoreBasedTopologySnapshotTest {
  "InMemoryTopologyStore" should {
    behave like topologySnapshot(() =>
      new InMemoryTopologyStore(
        TopologyStoreId.AuthorizedStore,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )
    )
  }
}

trait DbStoreBasedTopologySnapshotTest
    extends StoreBasedTopologySnapshotTest
    with DbTopologyStoreHelper {

  this: AsyncWordSpec with BaseTest with HasExecutionContext with DbTest =>

  "DbStoreBasedTopologySnapshot" should {
    behave like topologySnapshot(() => createTopologyStore(DefaultTestIdentities.synchronizerId))
  }

}

class DbStoreBasedTopologySnapshotTestPostgres
    extends DbStoreBasedTopologySnapshotTest
    with PostgresTest

class DbStoreBasedTopologySnapshotTestH2 extends DbStoreBasedTopologySnapshotTest with H2Test
