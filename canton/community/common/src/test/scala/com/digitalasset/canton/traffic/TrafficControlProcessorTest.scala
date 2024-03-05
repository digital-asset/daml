// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcastX.Broadcast
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  SetTrafficBalanceMessage,
  SignedProtocolMessage,
  TopologyTransactionsBroadcastX,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  DeliverError,
  MessageId,
  Recipients,
  SequencerErrors,
  SequencersOfDomain,
}
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactoryX
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  TestingIdentityFactoryX,
  TestingTopologyX,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.traffic.TrafficControlErrors.InvalidTrafficControlBalanceMessage
import com.digitalasset.canton.traffic.TrafficControlProcessor
import com.digitalasset.canton.traffic.TrafficControlProcessor.TrafficControlSubscriber
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class TrafficControlProcessorTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val domainId = DefaultTestIdentities.domainId
  private val participantId = DefaultTestIdentities.participant1

  private val ts1 = CantonTimestamp.ofEpochSecond(1)
  private val ts2 = CantonTimestamp.ofEpochSecond(2)
  private val ts3 = CantonTimestamp.ofEpochSecond(3)
  private val sc1 = SequencerCounter(1)
  private val sc2 = SequencerCounter(2)
  private val sc3 = SequencerCounter(3)

  private val domainCrypto = new TestingIdentityFactoryX(
    TestingTopologyX(),
    loggerFactory,
    dynamicDomainParameters = List.empty,
  )
    .forOwnerAndDomain(DefaultTestIdentities.sequencerIdX, domainId)

  private val dummySignature = SymbolicCrypto.emptySignature

  private val factoryX =
    new TopologyTransactionTestFactoryX(loggerFactory, initEc = parallelExecutionContext)

  private def mkTopoTx(): TopologyTransactionsBroadcastX = TopologyTransactionsBroadcastX.create(
    domainId,
    Seq(
      Broadcast(
        String255.tryCreate("some request"),
        List(factoryX.ns1k1_k1),
      )
    ),
    testedProtocolVersion,
  )

  private def mkSetBalance(
      signatureO: Option[Signature] = None
  ): SignedProtocolMessage[SetTrafficBalanceMessage] = {
    val setBalance = SetTrafficBalanceMessage(
      participantId,
      PositiveInt.one,
      NonNegativeLong.tryCreate(100),
      domainId,
      testedProtocolVersion,
    )

    signatureO match {
      case Some(signature) =>
        SignedProtocolMessage.from(
          setBalance,
          testedProtocolVersion,
          signature,
        )

      case None =>
        SignedProtocolMessage
          .trySignAndCreate(
            setBalance,
            domainCrypto.currentSnapshotApproximation,
            testedProtocolVersion,
          )
          .futureValue
    }
  }

  private def mkTrafficProcessor(): (
      TrafficControlProcessor,
      AtomicReference[mutable.Builder[CantonTimestamp, Seq[CantonTimestamp]]],
      AtomicReference[
        mutable.Builder[SetTrafficBalanceMessage, Seq[SetTrafficBalanceMessage]]
      ],
  ) = {
    val tcp = new TrafficControlProcessor(domainCrypto, domainId, loggerFactory)
    val observedTs = new AtomicReference(Seq.newBuilder[CantonTimestamp])
    val updates = new AtomicReference(Seq.newBuilder[SetTrafficBalanceMessage])

    tcp.subscribe(new TrafficControlSubscriber {
      override def observedTimestamp(timestamp: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): Unit = observedTs.updateAndGet(_ += timestamp)

      override def balanceUpdate(update: SetTrafficBalanceMessage)(implicit
          traceContext: TraceContext
      ): Unit = updates.updateAndGet(_ += update)
    })

    (tcp, observedTs, updates)
  }

  private def mkDeliver(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      batch: Batch[DefaultOpenEnvelope],
  ): Deliver[DefaultOpenEnvelope] =
    Deliver.create(sc, ts, domainId, None, batch, None, testedProtocolVersion)

  private def mkDeliverError(
      sc: SequencerCounter,
      ts: CantonTimestamp,
  ): DeliverError =
    DeliverError.create(
      sc,
      ts,
      domainId,
      MessageId.fromUuid(new UUID(0, 1)),
      SequencerErrors.SubmissionRequestMalformed("Some error"),
      testedProtocolVersion,
    )

  "the traffic control processor" should {
    "notify subscribers of all event timestamps" in {
      val batch = Batch.of(testedProtocolVersion, mkTopoTx() -> Recipients.cc(participantId))
      val events = NonEmpty(
        Seq,
        mkDeliver(sc1, ts1, batch),
        mkDeliverError(sc2, ts2),
        mkDeliver(sc3, ts3, batch),
      ).map(v => Traced(v))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      tcp.handle(events).futureValueUS

      observedTs.get().result() shouldBe Seq(ts1, ts2, ts3)
      updates.get().result() shouldBe Seq.empty
    }

    "notify subscribers of updates" in {
      val update = mkSetBalance()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfDomain))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      tcp.processSetTrafficBalanceEnvelopes(ts1, None, batch.envelopes).futureValueUS

      observedTs.get().result() shouldBe Seq.empty
      updates.get().result() shouldBe Seq(update.message)
    }

    "drop updates that do not target all sequencers" in {
      val update = mkSetBalance()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(participantId))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficBalanceEnvelopes(ts1, None, batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficControlBalanceMessage,
                _ should include("should be addressed to all the sequencers of a domain"),
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
      val update = mkSetBalance(Some(dummySignature))
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfDomain))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficBalanceEnvelopes(ts1, None, batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficControlBalanceMessage,
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
      val update = mkSetBalance()
      val batch =
        Batch.of(testedProtocolVersion, update -> Recipients.cc(SequencersOfDomain))

      val (tcp, observedTs, updates) = mkTrafficProcessor()

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        tcp.processSetTrafficBalanceEnvelopes(ts1, Some(ts2), batch.envelopes).futureValueUS,
        LogEntry.assertLogSeq(
          Seq(
            (
              _.shouldBeCantonError(
                InvalidTrafficControlBalanceMessage,
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
