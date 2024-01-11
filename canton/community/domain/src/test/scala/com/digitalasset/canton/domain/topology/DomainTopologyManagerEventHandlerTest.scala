// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.topology.store.InMemoryRegisterTopologyTransactionResponseStore
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
  RegisterTopologyTransactionResponseResult,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SendType,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

// TODO(#15303) Remove this test
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class DomainTopologyManagerEventHandlerTest extends AsyncWordSpec with BaseTest with MockitoSugar {
  private val requestId = String255.tryCreate("requestId")
  private val domainId = DomainId.tryFromString("da::default")
  private val participantId: ParticipantId = ParticipantId("p1")

  private val signedIdentityTransaction = SignedTopologyTransaction(
    TopologyStateUpdate(
      TopologyChangeOp.Add,
      TopologyStateUpdateElement(
        TopologyElementId.tryCreate("submissionId"),
        OwnerToKeyMapping(participantId, SymbolicCrypto.signingPublicKey("keyId")),
      ),
      testedProtocolVersion,
    ),
    SymbolicCrypto.signingPublicKey("keyId"),
    SymbolicCrypto.emptySignature,
    signedTransactionProtocolVersionRepresentative,
  )
  private val request = RegisterTopologyTransactionRequest
    .create(
      participantId,
      participantId,
      requestId,
      List(signedIdentityTransaction),
      domainId,
      testedProtocolVersion,
    )

  private val domainIdentityServiceResult =
    RegisterTopologyTransactionResponseResult.create(
      signedIdentityTransaction.uniquePath.toProtoPrimitive,
      RegisterTopologyTransactionResponseResult.State.Accepted,
      testedProtocolVersion,
    )
  private val response =
    RegisterTopologyTransactionResponse(
      participantId,
      participantId,
      requestId,
      List(domainIdentityServiceResult),
      domainId,
      testedProtocolVersion,
    )

  "DomainTopologyManagerEventHandler" should {
    "handle RegisterTopologyTransactionRequests and send resulting RegisterTopologyTransactionResponse back" in {
      val store = new InMemoryRegisterTopologyTransactionResponseStore()

      val sut = {

        val requestHandler =
          mock[DomainTopologyManagerRequestService.Handler]
        when(
          requestHandler.newRequest(
            any[Member],
            any[ParticipantId],
            any[List[SignedTopologyTransaction[TopologyChangeOp]]],
          )(any[TraceContext])
        )
          .thenReturn(FutureUnlessShutdown.pure(List(domainIdentityServiceResult)))

        val sequencerClientSend = mock[SequencerClientSend]
        when(
          sequencerClientSend.sendAsync(
            eqTo(
              Batch(
                List(
                  OpenEnvelope(response, Recipients.cc(response.requestedBy))(testedProtocolVersion)
                ),
                testedProtocolVersion,
              )
            ),
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[Option[AggregationRule]],
            any[SendCallback],
          )(any[TraceContext])
        ).thenAnswer {
          (
              _: Batch[DefaultOpenEnvelope],
              _: SendType,
              _: Option[CantonTimestamp],
              _: CantonTimestamp,
              _: MessageId,
              _: Option[AggregationRule],
              callback: SendCallback,
          ) =>
            callback.apply(UnlessShutdown.Outcome(SendResult.Success(null)))
            EitherT.rightT[Future, SendAsyncClientError](())
        }

        new DomainTopologyManagerEventHandler(
          store,
          requestHandler,
          sequencerClientSend,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

      val result = {
        val batch =
          Batch(
            List(
              OpenEnvelope(request, Recipients.cc(DomainTopologyManagerId(response.domainId)))(
                testedProtocolVersion
              )
            ),
            testedProtocolVersion,
          )
        sut.apply(
          Traced(
            Seq(
              Traced(
                Deliver.create(
                  SequencerCounter(0),
                  CantonTimestamp.MinValue,
                  domainId,
                  Some(MessageId.tryCreate("messageId")),
                  batch,
                  testedProtocolVersion,
                )
              )
            )
          )
        )
      }

      for {
        asyncResult <- result.onShutdown(fail())
        _ <- asyncResult.unwrap.onShutdown(fail())
        response <- store.getResponse(requestId)
      } yield response.isCompleted shouldBe true
    }
  }
}
