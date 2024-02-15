// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.{
  InMemoryFinalizedResponseStore,
  InMemoryMediatorDeduplicationStore,
  MediatorState,
}
import com.digitalasset.canton.domain.metrics.MediatorTestMetrics
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil.sequentialTraverse_
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, LfPartyId, SequencerCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future

class MediatorEventStageProcessorTest extends AsyncWordSpec with BaseTest with HasTestCloseContext {
  self =>
  private lazy val domainId = DefaultTestIdentities.domainId
  private lazy val mediatorId = DefaultTestIdentities.mediatorIdX
  private lazy val mediatorMetrics = MediatorTestMetrics
  private lazy val participantResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(10)
  private lazy val factory = new ExampleTransactionFactory()(domainId = domainId)
  private lazy val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree

  private lazy val initialDomainParameters = TestDomainParameters.defaultDynamic

  private lazy val defaultDynamicDomainParameters
      : List[DomainParameters.WithValidity[DynamicDomainParameters]] =
    List(
      DomainParameters.WithValidity(
        CantonTimestamp.Epoch,
        None,
        initialDomainParameters.tryUpdate(participantResponseTimeout = participantResponseTimeout),
      )
    )

  private class Env(
      dynamicDomainParameters: List[DomainParameters.WithValidity[DynamicDomainParameters]] =
        defaultDynamicDomainParameters
  ) {
    val identityClientEventHandler: UnsignedProtocolEventHandler = ApplicationHandler.success()
    val receivedEvents: mutable.Buffer[(RequestId, Seq[Traced[MediatorEvent]])] = mutable.Buffer()

    val state = new MediatorState(
      new InMemoryFinalizedResponseStore(loggerFactory),
      new InMemoryMediatorDeduplicationStore(loggerFactory, timeouts),
      mock[Clock],
      mediatorMetrics,
      testedProtocolVersion,
      CachingConfigs.defaultFinalizedMediatorRequestsCache,
      timeouts,
      loggerFactory,
    )

    val domainSyncCryptoApi: DomainSyncCryptoClient = new TestingIdentityFactoryX(
      TestingTopologyX(),
      loggerFactory,
      dynamicDomainParameters,
    ).forOwnerAndDomain(
      SequencerId(domainId),
      domainId,
    )

    lazy val noopDeduplicator: MediatorEventDeduplicator = new MediatorEventDeduplicator {
      override def rejectDuplicates(
          requestTimestamp: CantonTimestamp,
          envelopes: Seq[DefaultOpenEnvelope],
      )(implicit
          traceContext: TraceContext,
          callerCloseContext: CloseContext,
      ): Future[(Seq[DefaultOpenEnvelope], Future[Unit])] =
        Future.successful(envelopes -> Future.unit)
    }

    val processor = new MediatorEventsProcessor(
      state,
      domainSyncCryptoApi,
      identityClientEventHandler,
      (requestId, events, _tc) => {
        receivedEvents.append((requestId, events))
        HandlerResult.done
      },
      testedProtocolVersion,
      noopDeduplicator,
      MediatorTestMetrics,
      loggerFactory,
    )

    def deliver(timestamp: CantonTimestamp): Deliver[Nothing] =
      SequencerTestUtils.mockDeliver(0, timestamp, domainId)

    def request(timestamp: CantonTimestamp): Deliver[DefaultOpenEnvelope] =
      Deliver.create[DefaultOpenEnvelope](
        SequencerCounter(0),
        timestamp,
        domainId,
        None,
        Batch.of(
          testedProtocolVersion,
          (
            InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion),
            Recipients.cc(mediatorId),
          ),
        ),
        testedProtocolVersion,
      )

    def handle(events: RawProtocolEvent*): FutureUnlessShutdown[Unit] =
      processor
        .handle(events.map(e => Traced(e -> None)(traceContext)))
        .flatMap(_.unwrap)

    def receivedEventsFor(requestId: RequestId): Seq[MediatorEvent] =
      receivedEvents.filter(_._1 == requestId).flatMap(_._2).map(_.value).to(Seq)

    def receivedEventsAt(ts: CantonTimestamp): Seq[MediatorEvent] = receivedEvents
      .flatMap { case (_, tracedEvents) =>
        tracedEvents.map(_.value)
      }
      .filter(_.timestamp == ts)
      .to(Seq)
  }

  "raise alarms when receiving bad sequencer event batches" in {
    val env = new Env()

    val informeeMessage = mock[InformeeMessage]
    when(informeeMessage.domainId).thenReturn(domainId)
    when(informeeMessage.rootHash).thenReturn(RootHash(TestHash.digest(0)))

    val mediatorResponse = mock[MediatorResponse]
    when(mediatorResponse.representativeProtocolVersion).thenReturn(
      MediatorResponse.protocolVersionRepresentativeFor(testedProtocolVersion)
    )

    val signedConfirmationResponse =
      SignedProtocolMessage.from(mediatorResponse, testedProtocolVersion, Signature.noSignature)
    when(signedConfirmationResponse.message.domainId).thenReturn(domainId)
    val informeeMessageWithWrongDomainId = mock[InformeeMessage]
    when(informeeMessageWithWrongDomainId.domainId)
      .thenReturn(DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong::domain")))
    val badBatches = List(
      (
        Batch.of[ProtocolMessage](
          testedProtocolVersion,
          informeeMessage -> RecipientsTest.testInstance,
          informeeMessage -> RecipientsTest.testInstance,
        ),
        List("Received more than one mediator request."),
      ),
      (
        Batch.of[ProtocolMessage](
          testedProtocolVersion,
          informeeMessage -> RecipientsTest.testInstance,
          signedConfirmationResponse -> RecipientsTest.testInstance,
        ),
        List("Received both mediator requests and mediator responses."),
      ),
      (
        Batch.of[ProtocolMessage](
          testedProtocolVersion,
          informeeMessageWithWrongDomainId -> RecipientsTest.testInstance,
        ),
        List("Received messages with wrong domain ids: List(wrong::domain)"),
      ),
    )

    sequentialTraverse_(badBatches) { case (batch, expectedMessages) =>
      loggerFactory.assertLogs(
        env.processor.handle(
          Seq(
            Deliver.create(
              SequencerCounter(1),
              CantonTimestamp.Epoch,
              domainId,
              None,
              batch,
              testedProtocolVersion,
            )
          ).map(e => Traced(e -> None)(traceContext))
        ),
        expectedMessages map { error => (logEntry: LogEntry) =>
          logEntry.errorMessage should include(error)
        }: _*
      )
    }.onShutdown(fail()).map(_ => succeed)
  }

  "timeouts" should {
    "be raised if a pending event timeouts" in {
      val pendingRequestTs = CantonTimestamp.Epoch.plusMillis(1)
      val pendingRequestId = RequestId(pendingRequestTs)
      val pendingRequestF = responseAggregation(pendingRequestId)
      val env = new Env

      for {
        pendingRequest <- pendingRequestF
        _ <- env.state.add(pendingRequest)
        deliverTs = pendingRequestTs.add(participantResponseTimeout.unwrap).addMicros(1)
        _ <- env.handle(env.deliver(deliverTs)).onShutdown(fail())
      } yield {
        env.receivedEventsFor(pendingRequestId).loneElement should matchPattern {
          case MediatorEvent.Timeout(_, `deliverTs`, `pendingRequestId`) =>
        }
      }
    }

    "be raised if a pending event timeouts, taking dynamic domain parameters into account" in {

      val domainParameters = List(
        DomainParameters.WithValidity(
          CantonTimestamp.Epoch,
          Some(CantonTimestamp.ofEpochSecond(5)),
          initialDomainParameters.tryUpdate(participantResponseTimeout =
            NonNegativeFiniteDuration.tryOfSeconds(4)
          ),
        ),
        DomainParameters.WithValidity(
          CantonTimestamp.ofEpochSecond(5),
          None,
          initialDomainParameters.tryUpdate(participantResponseTimeout =
            NonNegativeFiniteDuration.tryOfSeconds(6)
          ),
        ),
      )

      def getRequest(requestTs: CantonTimestamp) = {
        val pendingRequestId = RequestId(requestTs)
        responseAggregation(pendingRequestId)
      }

      val pendingRequest1Ts = CantonTimestamp.Epoch.plusSeconds(2)
      val pendingRequest1Id = RequestId(pendingRequest1Ts)
      val pendingRequest1F = getRequest(pendingRequest1Ts) // times out at (2 + 4) = 6

      val pendingRequest2Ts = CantonTimestamp.Epoch.plusSeconds(6)
      val pendingRequest2Id = RequestId(pendingRequest2Ts)

      /*
        The following times out at (6 + 6) = 12
        If dynamic domain parameters are not taken into account, it would be
        incorrectly marked as timed out at 11
       */
      val pendingRequest2F = getRequest(pendingRequest2Ts)

      val deliver1Ts = CantonTimestamp.Epoch.plusSeconds(11)
      val deliver2Ts = CantonTimestamp.Epoch.plusSeconds(12).addMicros(1)

      def test(
          deliverTs: CantonTimestamp,
          expectedEvents: Set[MediatorEvent],
      ): Future[Assertion] = {
        val env = new Env(domainParameters)

        for {
          pendingRequest1 <- pendingRequest1F
          pendingRequest2 <- pendingRequest2F
          _ <- env.state.add(pendingRequest1)
          _ <- env.state.add(pendingRequest2)
          _ <- env.handle(env.deliver(deliverTs)).onShutdown(fail())
        } yield env.receivedEventsAt(deliverTs).toSet shouldBe expectedEvents
      }

      for {
        assertion1 <- test(
          deliver1Ts,
          Set(MediatorEvent.Timeout(SequencerCounter(0), deliver1Ts, pendingRequest1Id)),
        )
        assertion2 <- test(
          deliver2Ts,
          Set(
            MediatorEvent.Timeout(SequencerCounter(0), deliver2Ts, pendingRequest1Id),
            MediatorEvent.Timeout(SequencerCounter(0), deliver2Ts, pendingRequest2Id),
          ),
        )
      } yield (assertion1, assertion2) shouldBe (succeed, succeed)
    }

    "be raised for a request that is potentially created during the batch of events" in {
      val env = new Env
      val firstRequestTs = CantonTimestamp.Epoch.plusMillis(1)
      val requestId = RequestId(firstRequestTs)
      val timesOutAt = firstRequestTs.add(participantResponseTimeout.unwrap).addMicros(1)

      for {
        _ <- env.handle(env.request(firstRequestTs), env.deliver(timesOutAt)).onShutdown(fail())
      } yield {
        env.receivedEventsFor(requestId) should matchPattern {
          case Seq(
                MediatorEvent.Request(
                  _,
                  `firstRequestTs`,
                  InformeeMessage(_, _),
                  _,
                  _,
                ),
                MediatorEvent.Timeout(_, `timesOutAt`, `requestId`),
              ) =>
        }
      }
    }
  }

  private def responseAggregation(requestId: RequestId): Future[ResponseAggregation[?]] = {
    val mockTopologySnapshot = mock[TopologySnapshot]
    when(mockTopologySnapshot.consortiumThresholds(any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (parties: Set[LfPartyId]) =>
        Future.successful(parties.map(x => x -> PositiveInt.one).toMap)
      }
    ResponseAggregation.fromRequest(
      requestId,
      InformeeMessage(fullInformeeTree, Signature.noSignature)(testedProtocolVersion),
      mockTopologySnapshot,
    )
  }
}
