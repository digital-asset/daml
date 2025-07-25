// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficPurchasedMessage,
  SignedProtocolMessage,
  TopologyTransactionsBroadcast,
}
import com.digitalasset.canton.sequencing.WithCounter
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.InvalidTrafficPurchasedMessage
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor.TrafficControlSubscriber
import com.digitalasset.canton.sequencing.traffic.{TrafficControlProcessor, TrafficReceipt}
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class TrafficControlProcessorTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private val participantId = DefaultTestIdentities.participant1

  private val ts1 = CantonTimestamp.ofEpochSecond(1)
  private val ts2 = CantonTimestamp.ofEpochSecond(2)
  private val ts3 = CantonTimestamp.ofEpochSecond(3)
  private val sc1 = SequencerCounter(1)
  private val sc2 = SequencerCounter(2)
  private val sc3 = SequencerCounter(3)

  private val synchronizerCrypto = TestingTopology(synchronizerParameters = List.empty)
    .build(loggerFactory)
    .forOwnerAndSynchronizer(DefaultTestIdentities.sequencerId, synchronizerId)

  private val dummySignature = SymbolicCrypto.emptySignature

  private val factory =
    new TopologyTransactionTestFactory(loggerFactory, initEc = parallelExecutionContext)

  private lazy val topoTx: TopologyTransactionsBroadcast = TopologyTransactionsBroadcast(
    synchronizerId,
    List(factory.ns1k1_k1),
  )

  private def mkSetTrafficPurchased(
      signatureO: Option[Signature] = None
  ): SignedProtocolMessage[SetTrafficPurchasedMessage] = {
    val setTrafficPurchased = SetTrafficPurchasedMessage(
      participantId,
      PositiveInt.one,
      NonNegativeLong.tryCreate(100),
      synchronizerId,
    )

    signatureO match {
      case Some(signature) =>
        SignedProtocolMessage.from(
          setTrafficPurchased,
          signature,
        )

      case None =>
        SignedProtocolMessage
          .trySignAndCreate(
            setTrafficPurchased,
            synchronizerCrypto.currentSnapshotApproximation,
          )
          .failOnShutdown
          .futureValue
    }
  }

  private def mkTrafficProcessor(): (
      TrafficControlProcessor,
      AtomicReference[mutable.Builder[CantonTimestamp, Seq[CantonTimestamp]]],
      AtomicReference[
        mutable.Builder[SetTrafficPurchasedMessage, Seq[SetTrafficPurchasedMessage]]
      ],
  ) = {
    val tcp = new TrafficControlProcessor(
      synchronizerCrypto,
      synchronizerId,
      Option.empty[CantonTimestamp],
      loggerFactory,
    )
    val observedTs = new AtomicReference(Seq.newBuilder[CantonTimestamp])
    val updates = new AtomicReference(Seq.newBuilder[SetTrafficPurchasedMessage])

    tcp.subscribe(new TrafficControlSubscriber {
      override def observedTimestamp(timestamp: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): Unit = observedTs.updateAndGet(_ += timestamp)

      override def trafficPurchasedUpdate(
          update: SetTrafficPurchasedMessage,
          sequencingTimestamp: CantonTimestamp,
      )(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure(updates.updateAndGet(_ += update))
    })

    (tcp, observedTs, updates)
  }

  private def mkDeliver(
      ts: CantonTimestamp,
      batch: Batch[DefaultOpenEnvelope],
  ): Deliver[DefaultOpenEnvelope] =
    Deliver.create(
      None,
      ts,
      synchronizerId,
      None,
      batch,
      None,
      Option.empty[TrafficReceipt],
    )

  private def mkDeliverError(
      ts: CantonTimestamp
  ): DeliverError =
    DeliverError.create(
      None,
      ts,
      synchronizerId,
      MessageId.fromUuid(new UUID(0, 1)),
      SequencerErrors.SubmissionRequestRefused("Some error"),
      Option.empty[TrafficReceipt],
    )

  "the traffic control processor" should {
    "notify subscribers of all event timestamps" in {
      val batch = Batch.of(testedProtocolVersion, topoTx -> Recipients.cc(participantId))
      val events = Traced(
        Seq(
          sc1 -> mkDeliver(ts1, batch),
          sc2 -> mkDeliverError(ts2),
          sc3 -> mkDeliver(ts3, batch),
        ).map { case (counter, e) => WithCounter(counter, Traced(e)) }
      )

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      tcp(events).futureValueUS.unwrap.futureValueUS

      observedTs.get().result() shouldBe Seq(ts1, ts2, ts3)
      updates.get().result() shouldBe Seq.empty
    }

    "notify subscribers of updates" in {
      val update = mkSetTrafficPurchased()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfSynchronizer))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      tcp.processSetTrafficPurchasedEnvelopes(ts1, None, batch.envelopes).futureValueUS

      observedTs.get().result() shouldBe Seq.empty
      updates.get().result() shouldBe Seq(update.message)
    }

    "drop updates that do not target all sequencers" in {
      val update = mkSetTrafficPurchased()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(participantId))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficPurchasedEnvelopes(ts1, None, batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficPurchasedMessage,
                _ should include("should be addressed to all the sequencers of a synchronizer"),
              ),
              "invalid recipients",
            )
          )
        ),
      )

      observedTs.get().result() shouldBe Seq(ts1)
      updates.get().result() shouldBe Seq.empty
    }

    "drop updates with invalid signatures" in {
      val update = mkSetTrafficPurchased(Some(dummySignature))
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfSynchronizer))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficPurchasedEnvelopes(ts1, None, batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficPurchasedMessage,
                _ should (include(
                  "signature threshold not reached"
                ) and include regex raw"Key \S+ used to generate signature is not a valid key for SequencerGroup"),
              ),
              "invalid signatures",
            )
          )
        ),
      )

      observedTs.get().result() shouldBe Seq(ts1)
      updates.get().result() shouldBe Seq.empty
    }

    "drop updates with invalid timestamp of signing key" in {
      val update = mkSetTrafficPurchased()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfSynchronizer))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficPurchasedEnvelopes(ts1, Some(ts2), batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficPurchasedMessage,
                _ should include(
                  s"the timestamp of the topology (Some($ts2)) is not set to the event timestamp ($ts1)"
                ),
              ),
              "invalid timestamp of signing key",
            )
          )
        ),
      )

      observedTs.get().result() shouldBe Seq(ts1)
      updates.get().result() shouldBe Seq.empty
    }
  }
}
