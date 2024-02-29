// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.db.DbTopologyStoreX
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, BaseTestWordSpec, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class BaseDomainTopologyClientTest extends BaseTestWordSpec {

  private class TestClient() extends BaseDomainTopologyClientX {

    override def protocolVersion: ProtocolVersion = testedProtocolVersion

    override protected def futureSupervisor: FutureSupervisor = FutureSupervisor.Noop
    override def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
    override def trySnapshot(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): TopologySnapshotLoader =
      ???
    override def domainId: DomainId = ???

    def advance(ts: CantonTimestamp): Unit = {
      this
        .observed(SequencedTime(ts), EffectiveTime(ts), SequencerCounter(0), List())
        .failOnShutdown(s"advance to $ts")
        .futureValue
    }
    override implicit val executionContext: ExecutionContext =
      DirectExecutionContext(noTracingLogger)
    override protected def loggerFactory: NamedLoggerFactory =
      BaseDomainTopologyClientTest.this.loggerFactory
    override protected def clock: Clock = ???
    override def currentSnapshotApproximation(implicit
        traceContext: TraceContext
    ): TopologySnapshotLoader = ???
    override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = ???
  }

  "waiting for snapshots" should {

    val ts1 = CantonTimestamp.Epoch
    val ts2 = ts1.plusSeconds(60)

    "announce snapshot if there is one" in {
      val tc = new TestClient()
      tc.advance(ts1)
      tc.snapshotAvailable(ts1) shouldBe true
      tc.snapshotAvailable(ts2) shouldBe false
      tc.advance(ts2)
      tc.snapshotAvailable(ts2) shouldBe true
    }

    "correctly get notified" in {
      val tc = new TestClient()
      val wt = tc.awaitTimestamp(ts2, true)
      wt match {
        case Some(fut) =>
          tc.advance(ts1)
          fut.isCompleted shouldBe false
          tc.advance(ts2)
          fut.isCompleted shouldBe true
        case None => fail("expected future")
      }
    }

    "just return a none if snapshot already exists" in {
      val tc = new TestClient()
      tc.advance(ts1)
      val wt = tc.awaitTimestamp(ts1, waitForEffectiveTime = true)
      wt shouldBe None
    }

  }

}

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
trait StoreBasedTopologySnapshotTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import EffectiveTimeTestHelpers.*

  def topologySnapshot(mk: () => TopologyStoreX[TopologyStoreId]): Unit = {

    val factory = new TestingOwnerWithKeysX(
      DefaultTestIdentities.participant1,
      loggerFactory,
      parallelExecutionContext,
    )
    import DefaultTestIdentities.*
    import factory.*
    import factory.TestingTransactions.*

    lazy val party1participant1 = mkAdd(
      PartyToParticipantX(
        party1,
        None,
        PositiveInt.one,
        Seq(HostingParticipant(participant1, Confirmation)),
        groupAddressing = false,
      )
    )
    lazy val party2participant1_2 = mkAdd(
      PartyToParticipantX(
        party2,
        None,
        PositiveInt.one,
        Seq(
          HostingParticipant(participant1, Submission),
          HostingParticipant(participant2, Submission),
        ),
        groupAddressing = false,
      )
    )

    class Fixture() {
      val store = mk()
      val client =
        new StoreBasedDomainTopologyClientX(
          mock[Clock],
          domainId,
          testedProtocolVersion,
          store,
          StoreBasedDomainTopologyClient.NoPackageDependencies,
          DefaultProcessingTimeouts.testing,
          FutureSupervisor.Noop,
          loggerFactory,
        )

      def add(
          timestamp: CantonTimestamp,
          transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
      ): Future[Unit] = {

        for {
          _ <- store.update(
            SequencedTime(timestamp),
            EffectiveTime(timestamp),
            removeMapping = transactions.map(_.mapping.uniqueKey).toSet,
            removeTxs = transactions.map(_.hash).toSet,
            additions = transactions.map(ValidatedTopologyTransactionX(_)),
          )
          _ <- client
            .observed(timestamp, timestamp, SequencerCounter(1), transactions)
            .failOnShutdown(s"observe timestamp $timestamp")
        } yield ()
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
        keys <- sp.signingKeys(participant1)
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
        keys <- recent.signingKeys(participant1)
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

    "properly deals with participants with lower domain privileges" in {
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
        keysMa <- snapshotA.signingKeys(mediatorIdX)
        keysMb <- snapshotB.signingKeys(mediatorIdX)
        keysSa <- snapshotA.signingKeys(sequencerIdX)
        keysSb <- snapshotB.signingKeys(sequencerIdX)
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
          .valueOrFail("No permission for particiant1 in snapshotA")
          .permission shouldBe ParticipantPermission.Submission
        partPermB
          .valueOrFail("No permission for particiant1 in snapshotB")
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
      new InMemoryTopologyStoreX(
        TopologyStoreId.AuthorizedStore,
        loggerFactory,
        timeouts,
      )
    )
  }
}

trait DbStoreBasedTopologySnapshotTest extends StoreBasedTopologySnapshotTest {

  this: AsyncWordSpec with BaseTest with HasExecutionContext with DbTest =>

  protected def pureCryptoApi: CryptoPureApi

  "DbStoreBasedTopologySnapshot" should {
    behave like topologySnapshot(() =>
      new DbTopologyStoreX(
        storage,
        TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
        timeouts,
        loggerFactory,
      )
    )
  }
}
