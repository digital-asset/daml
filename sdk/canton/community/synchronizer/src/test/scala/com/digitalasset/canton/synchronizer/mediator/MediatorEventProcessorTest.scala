// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{LocalRejectError, RequestId, RootHash}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  HandlerResult,
  TracedProtocolEvent,
  UnsignedEnvelopeBox,
  WithCounter,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, SequencerCounter}
import org.mockito.ArgumentMatchers.eq as isEq

import java.util.UUID

class MediatorEventProcessorTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with HasTestCloseContext {

  private def mkEventProcessor(
      isUniqueDeduplicationResult: Boolean = true
  ): (MediatorEventsProcessor, MediatorEventDeduplicator, MediatorEventHandler) = {
    val eventHandler = mock[MediatorEventHandler]
    when(eventHandler.handleMediatorEvent(any[MediatorEvent])(anyTraceContext))
      .thenAnswer(HandlerResult.done)
    when(eventHandler.observeTimestampWithoutEvent(any[CantonTimestamp])(anyTraceContext))
      .thenAnswer(HandlerResult.done)

    val deduplicator = mock[MediatorEventDeduplicator]
    when(
      deduplicator.rejectDuplicate(
        any[CantonTimestamp],
        any[MediatorConfirmationRequest],
        any[Seq[DefaultOpenEnvelope]],
      )(anyTraceContext, any[CloseContext])
    ).thenAnswer(
      FutureUnlessShutdown.pure((isUniqueDeduplicationResult, FutureUnlessShutdown.unit))
    )

    val processor = new MediatorEventsProcessor(
      identityClientEventHandler =
        ApplicationHandler.success[UnsignedEnvelopeBox, DefaultOpenEnvelope](),
      eventHandler,
      deduplicator,
      loggerFactory,
    )
    (processor, deduplicator, eventHandler)
  }

  private def ts(i: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(i.toLong)

  private lazy val uuids: Seq[UUID] = List(
    "51f3ffff-9248-453b-807b-91dd7ed23298",
    "c0175d4a-def2-481e-a979-ae9d335b5d35",
  ).map(UUID.fromString)

  private lazy val request: Seq[OpenEnvelope[MediatorConfirmationRequest]] =
    uuids.map(mkMediatorRequest(_))

  private def mkEvent(
      ts: CantonTimestamp,
      envelopes: DefaultOpenEnvelope*
  ): (TracedProtocolEvent) =
    WithCounter(
      SequencerCounter.One, // not relevant
      Traced(
        Deliver.create(
          previousTimestamp = None, // not relevant
          timestamp = ts,
          synchronizerId = synchronizerId,
          messageIdO = None, // not relevant
          batch = Batch(envelopes.toList, testedProtocolVersion),
          topologyTimestampO = None, // not relevant
          trafficReceipt = None, // not relevant
          protocolVersion = testedProtocolVersion,
        )
      )(TraceContext.createNew()),
    )

  private def mkMediatorRequest(
      uuid: UUID,
      synchronizerId: SynchronizerId = synchronizerId,
  ): OpenEnvelope[MediatorConfirmationRequest] = {
    import Pretty.*

    val mediatorRequest = mock[MediatorConfirmationRequest]
    when(mediatorRequest.requestUuid).thenReturn(uuid)
    when(mediatorRequest.synchronizerId).thenReturn(synchronizerId)
    when(mediatorRequest.pretty).thenReturn(
      prettyOfClass[MediatorConfirmationRequest](param("uuid", _.requestUuid))
    )

    mkDefaultOpenEnvelope(mediatorRequest)
  }

  private def mkDefaultOpenEnvelope[A <: ProtocolMessage](protocolMessage: A): OpenEnvelope[A] =
    OpenEnvelope(protocolMessage, Recipients.cc(daMediator))(testedProtocolVersion)

  private lazy val response: OpenEnvelope[SignedProtocolMessage[ConfirmationResponses]] = {
    val confirmationResponses = ConfirmationResponses.tryCreate(
      RequestId(CantonTimestamp.now()),
      RootHash(TestHash.digest("txid3")),
      synchronizerId,
      participant1,
      NonEmpty.mk(
        Seq,
        ConfirmationResponse.tryCreate(
          None,
          LocalRejectError.MalformedRejects.Payloads
            .Reject("test message")
            .toLocalReject(testedProtocolVersion),
          Set.empty,
        ),
      ),
      testedProtocolVersion,
    )

    val message = SignedProtocolMessage(
      TypedSignedProtocolMessageContent(confirmationResponses, testedProtocolVersion),
      NonEmpty(Seq, SymbolicCrypto.emptySignature),
      testedProtocolVersion,
    )
    mkDefaultOpenEnvelope(message)
  }

  "The event processor" should {
    "filter out envelopes for the wrong synchronizer" in {
      val (processor, deduplicator, handler) = mkEventProcessor()

      val matchingSynchronizer = mkMediatorRequest(uuids(0))
      val mismatchingSynchronizer =
        mkMediatorRequest(uuids(1), SynchronizerId.tryFromString("invalid::synchronizer"))

      val event1 = mkEvent(
        ts(0),
        matchingSynchronizer,
      )
      val event2 = mkEvent(
        ts(1),
        matchingSynchronizer,
        mismatchingSynchronizer,
      )
      val event3 = mkEvent(
        ts(2),
        mismatchingSynchronizer,
      )

      val filtered =
        loggerFactory.assertLogs(
          processor
            .filterEnvelopesForSynchronizer(
              NonEmpty(
                Seq,
                event1,
                event2,
                event3,
              )
            ),
          _.errorMessage should include(
            "Received messages with wrong synchronizer ids: List(invalid::synchronizer)"
          ),
          _.errorMessage should include(
            "Received messages with wrong synchronizer ids: List(invalid::synchronizer)"
          ),
        )
      filtered.forgetNE should contain theSameElementsInOrderAs Seq(
        (event1, Seq(matchingSynchronizer)),
        (event2, Seq(matchingSynchronizer)),
        (event3, Seq.empty),
      )
      verifyZeroInteractions(deduplicator, handler)
    }

    "ignore submissions with multiple confirmation requests" in {
      val (processor, deduplicator, handler) = mkEventProcessor()
      val envelopes = Seq(request(0), request(1))
      val event = mkEvent(
        ts(0),
        envelopes*
      )

      loggerFactory.assertLogs(
        processor.handle(Seq(event)).futureValueUS.unwrap.futureValueUS,
        _.shouldBeCantonError(
          MediatorError.MalformedMessage.code,
          _ should include(
            "Received more than one mediator confirmation request."
          ),
        ),
      )

      verify(handler).observeTimestampWithoutEvent(isEq(ts(0)))(isEq(event.traceContext))
      verifyNoMoreInteractions(deduplicator, handler)
    }

    "ignore submissions with mixed requests and responses" in {
      val (processor, deduplicator, handler) = mkEventProcessor()
      val envelopes = Seq(request(0), response)
      val event = mkEvent(
        ts(0),
        envelopes*
      )

      loggerFactory.assertLogs(
        processor.handle(Seq(event)).futureValueUS.unwrap.futureValueUS,
        _.shouldBeCantonError(
          MediatorError.MalformedMessage.code,
          _ should include(
            "Received both mediator confirmation requests and confirmation responses."
          ),
        ),
      )

      verify(handler).observeTimestampWithoutEvent(isEq(ts(0)))(isEq(event.traceContext))
      verifyNoMoreInteractions(deduplicator, handler)
    }

    "pass submissions with no confirmation requests only to the event handler" in {
      val (processor, deduplicator, handler) = mkEventProcessor()
      val envelopes = Seq(response, response, response)
      val event = mkEvent(ts(0), envelopes*)

      processor.handle(Seq(event)).futureValueUS.unwrap.futureValueUS

      verifyZeroInteractions(deduplicator)
      verify(handler, times(3)).handleMediatorEvent(any[MediatorEvent.Response])(
        isEq(event.traceContext)
      )
      verifyNoMoreInteractions(deduplicator, handler)
    }

    "trigger the timeout check for submissions with no mediator events" in {
      val (processor, deduplicator, handler) = mkEventProcessor()
      val event = mkEvent(ts(0))

      processor.handle(Seq(event)).futureValueUS.unwrap.futureValueUS

      verify(handler).observeTimestampWithoutEvent(isEq(ts(0)))(isEq(event.traceContext))
      verifyNoMoreInteractions(deduplicator, handler)
    }

    "handle timeouts when requests are considered duplicate" in {
      val (processor, deduplicator, handler) = mkEventProcessor(isUniqueDeduplicationResult = false)
      val event = mkEvent(ts(0), request(0))

      processor.handle(Seq(event)).futureValueUS.unwrap.futureValueUS

      verify(deduplicator).rejectDuplicate(
        isEq(ts(0)),
        isEq(request(0).protocolMessage),
        isEq(Seq.empty),
      )(
        anyTraceContext,
        any[CloseContext],
      )
      verify(handler).observeTimestampWithoutEvent(isEq(ts(0)))(isEq(event.traceContext))
      verifyNoMoreInteractions(deduplicator, handler)
    }

    "properly detect duplicates between multiple consecutive events that are processed in a batch" in {
      val (processor, deduplicator, handler) = mkEventProcessor()

      val event1 = mkEvent(ts(0), request(0))
      val event2 = mkEvent(ts(1), request(0))
      val event3 = mkEvent(ts(10), request(0))

      when(
        deduplicator.rejectDuplicate(
          isEq(ts(1)),
          isEq(request(0).protocolMessage),
          isEq(Seq.empty),
        )(isEq(event2.traceContext), any[CloseContext])
      )
        .thenReturn(FutureUnlessShutdown.pure(false -> FutureUnlessShutdown.unit))
      processor.handle(Seq(event1, event2, event3)).futureValueUS.unwrap.futureValueUS

      // check that the deduplication check was executed
      verify(deduplicator).rejectDuplicate(
        isEq(event1.value.timestamp),
        isEq(request(0).protocolMessage),
        isEq(Seq.empty),
      )(
        isEq(event1.traceContext),
        any[CloseContext],
      )

      verify(deduplicator)
        .rejectDuplicate(
          isEq(event2.value.timestamp),
          isEq(request(0).protocolMessage),
          isEq(Seq.empty),
        )(
          isEq(event2.traceContext),
          any[CloseContext],
        )

      verify(deduplicator).rejectDuplicate(
        isEq(event3.value.timestamp),
        isEq(request(0).protocolMessage),
        isEq(Seq.empty),
      )(
        isEq(event3.traceContext),
        any[CloseContext],
      )

      // check that the events were handed to the event handler
      verify(handler).handleMediatorEvent(
        isEq(
          MediatorEvent.Request(
            SequencerCounter.One, // this is hardcoded in mkEvent and not relevent for this test
            event1.value.timestamp,
            request(0),
            List.empty,
            batchAlsoContainsTopologyTransaction = false,
          )
        )
      )(isEq(event1.traceContext))

      verify(handler).handleMediatorEvent(
        isEq(
          MediatorEvent.Request(
            SequencerCounter.One, // this is hardcoded in mkEvent and not relevent for this test
            event3.value.timestamp,
            request(0),
            List.empty,
            batchAlsoContainsTopologyTransaction = false,
          )
        )
      )(isEq(event3.traceContext))

      verifyNoMoreInteractions(deduplicator, handler)
    }

    "handle multiple valid submissions" in {
      val (processor, deduplicator, handler) = mkEventProcessor()

      val event1 = mkEvent(ts(0), request(0))
      val event2 = mkEvent(ts(1), request(1))
      // send an additional topology broadcast so that we can test the timeout handling
      val event3 = mkEvent(
        ts(2),
        mkDefaultOpenEnvelope(
          TopologyTransactionsBroadcast(synchronizerId, Seq.empty, testedProtocolVersion)
        ),
      )

      processor.handle(Seq(event1, event2, event3)).futureValueUS.unwrap.futureValueUS

      // check that the deduplication check was executed
      verify(deduplicator).rejectDuplicate(
        isEq(event1.value.timestamp),
        isEq(request(0).protocolMessage),
        isEq(Seq.empty),
      )(
        isEq(event1.traceContext),
        any[CloseContext],
      )

      verify(deduplicator).rejectDuplicate(
        isEq(event2.value.timestamp),
        isEq(request(1).protocolMessage),
        isEq(Seq.empty),
      )(
        isEq(event2.traceContext),
        any[CloseContext],
      )

      // check that the events were handed to the event handler
      verify(handler).handleMediatorEvent(
        isEq(
          MediatorEvent.Request(
            SequencerCounter.One, // this is hardcoded in mkEvent and not relevent for this test
            event1.value.timestamp,
            request(0),
            List.empty,
            batchAlsoContainsTopologyTransaction = false,
          )
        )
      )(isEq(event1.traceContext))

      verify(handler).handleMediatorEvent(
        isEq(
          MediatorEvent.Request(
            SequencerCounter.One, // this is hardcoded in mkEvent and not relevent for this test
            event2.value.timestamp,
            request(1),
            List.empty,
            batchAlsoContainsTopologyTransaction = false,
          )
        )
      )(isEq(event2.traceContext))

      verify(handler).observeTimestampWithoutEvent(isEq(event3.value.timestamp))(
        isEq(event3.traceContext)
      )
      verifyNoMoreInteractions(deduplicator, handler)
    }
  }
}
