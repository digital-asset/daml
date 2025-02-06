// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.integrations.state

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

trait SequencerStateManagerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with BeforeAndAfterAll
    with ProtocolVersionChecksAsyncWordSpec {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var actorSystem: ActorSystem = _
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var materializer: Materializer = _

  override def beforeAll(): Unit = {
    actorSystem = ActorSystem("sequencer-store-test")
    materializer = Materializer(actorSystem)
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally {
      materializer.shutdown()
      Await.result(actorSystem.terminate(), 10.seconds)
    }

  def sequencerStateManagerStore(mk: () => SequencerStateManagerStore): Unit = {
    val alice = ParticipantId(UniqueIdentifier.tryCreate("participant", "alice"))
    val bob = ParticipantId(UniqueIdentifier.tryCreate("participant", "bob"))
    val carlos = ParticipantId(UniqueIdentifier.tryCreate("participant", "carlos"))

    def ts(epochSeconds: Int): CantonTimestamp =
      CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

    val t1 = ts(1)
    val t2 = ts(2)
    val t3 = ts(3)
    val t4 = ts(4)

    "read at timestamp" should {
      "hydrate a correct empty state when there have been no updates" in {
        val store = mk()
        (for {
          lowerBoundAndInFlightAggregations <- store.readInFlightAggregations(
            CantonTimestamp.Epoch
          )
        } yield {
          val inFlightAggregations = lowerBoundAndInFlightAggregations
          inFlightAggregations shouldBe Map.empty
        }).failOnShutdown
      }

      "reconstruct the aggregation state" in withNewTraceContext { implicit traceContext =>
        val store = mk()
        val aggregationId1 = AggregationId(TestHash.digest(1))
        val aggregationId2 = AggregationId(TestHash.digest(2))
        val aggregationId3 = AggregationId(TestHash.digest(3))
        val rule = AggregationRule(
          NonEmpty(Seq, alice, bob),
          threshold = PositiveInt.tryCreate(2),
          testedProtocolVersion,
        )
        val signatureAlice1 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice1"),
          alice.fingerprint,
        )
        val signatureAlice2 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice2"),
          alice.fingerprint,
        )
        val signatureAlice3 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice3"),
          alice.fingerprint,
        )
        val signatureBob = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureBob"),
          bob.fingerprint,
        )

        val inFlightAggregation1 = InFlightAggregation(
          rule,
          t4,
          alice -> AggregationBySender(
            t2,
            Seq(Seq(signatureAlice1), Seq(signatureAlice2, signatureAlice3)),
          ),
          bob -> AggregationBySender(t3, Seq(Seq(signatureBob), Seq.empty)),
        )
        val inFlightAggregation2 = InFlightAggregation(rule, t3)
        val inFlightAggregation3 = InFlightAggregation(
          rule,
          t4,
          alice -> AggregationBySender(
            t4.immediatePredecessor,
            Seq(Seq(signatureAlice1), Seq.empty, Seq(signatureAlice2)),
          ),
        )

        (for {
          _ <- store.addInFlightAggregationUpdates(
            Map(
              aggregationId1 -> inFlightAggregation1.asUpdate,
              aggregationId2 -> inFlightAggregation2.asUpdate,
              aggregationId3 -> inFlightAggregation3.asUpdate,
            )
          )
          head2pred <- store.readInFlightAggregations(t2.immediatePredecessor)
          head2 <- store.readInFlightAggregations(t2)
          head3 <- store.readInFlightAggregations(t3)
          head4pred <- store.readInFlightAggregations(t4.immediatePredecessor)
          head4 <- store.readInFlightAggregations(t4)
        } yield {
          // All aggregations by sender have later timestamps and the in-flight aggregations are therefore considered inexistent
          head2pred shouldBe Map.empty
          head2 shouldBe Map(
            aggregationId1 -> inFlightAggregation1
              .focus(_.aggregatedSenders)
              // bob's aggregation happened later
              .modify(_.removed(bob))
          )
          head3 shouldBe Map(
            aggregationId1 -> inFlightAggregation1
          )
          head4pred shouldBe Map(
            aggregationId1 -> inFlightAggregation1,
            // aggregationId2 has already expired
            aggregationId3 -> inFlightAggregation3,
          )
          // All in-flight aggregations have expired
          head4 shouldBe Map.empty
        }).failOnShutdown
      }
    }

    "aggregation expiry" should {
      "delete all aggregation whose max sequencing time has elapsed" in {
        val store = mk()
        val aggregationId1 = AggregationId(TestHash.digest(1))
        val aggregationId2 = AggregationId(TestHash.digest(2))
        val rule = AggregationRule(
          NonEmpty(Seq, alice, bob, carlos),
          threshold = PositiveInt.tryCreate(2),
          testedProtocolVersion,
        )
        val signatureAlice1 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice1"),
          alice.fingerprint,
        )
        val signatureAlice2 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice2"),
          alice.fingerprint,
        )
        val signatureAlice3 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice3"),
          alice.fingerprint,
        )
        val signatureBob = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureBob"),
          bob.fingerprint,
        )

        val inFlightAggregation1 = InFlightAggregation(
          rule,
          t3,
          alice -> AggregationBySender(
            t1,
            Seq(Seq(signatureAlice1), Seq(signatureAlice2, signatureAlice3)),
          ),
          bob -> AggregationBySender(t2, Seq(Seq(signatureBob), Seq.empty)),
        )
        val inFlightAggregation2 = InFlightAggregation(
          rule,
          t3.immediateSuccessor,
          alice -> AggregationBySender(t2, Seq.fill(3)(Seq.empty)),
        )

        (for {
          _ <- store.addInFlightAggregationUpdates(
            Map(
              aggregationId1 -> inFlightAggregation1.asUpdate,
              aggregationId2 -> inFlightAggregation2.asUpdate,
            )
          )
          _ <- store.pruneExpiredInFlightAggregations(t2)
          head2 <- store.readInFlightAggregations(t2)
          _ <- store.pruneExpiredInFlightAggregations(t3)
          head3 <- store.readInFlightAggregations(t3)
          // We're using here the ability to read actually inconsistent data (for crash recovery)
          // for an already expired block to check that the expiry actually deletes the data.
          head2expired <- store.readInFlightAggregations(t2)
        } yield {
          head2 shouldBe Map(
            aggregationId1 -> inFlightAggregation1,
            aggregationId2 -> inFlightAggregation2,
          )
          head3 shouldBe Map(
            aggregationId2 -> inFlightAggregation2
          )
          head2expired shouldBe Map(
            aggregationId2 -> inFlightAggregation2
          )
        }).failOnShutdown
      }
    }
  }
}
