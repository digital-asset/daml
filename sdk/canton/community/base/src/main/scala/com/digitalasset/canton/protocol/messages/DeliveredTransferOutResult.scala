// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult.InvalidTransferOutResult
import com.digitalasset.canton.protocol.{SourceDomainId, TransferId}
import com.digitalasset.canton.sequencing.RawProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  SignedContent,
  WithOpeningErrors,
}

final case class DeliveredTransferOutResult(result: SignedContent[Deliver[DefaultOpenEnvelope]])
    extends PrettyPrinting {

  val unwrap: ConfirmationResultMessage = result.content match {
    case Deliver(_, _, _, _, Batch(envelopes), _, _) =>
      val transferOutResults =
        envelopes
          .mapFilter(env =>
            ProtocolMessage.select[SignedProtocolMessage[ConfirmationResultMessage]](env)
          )
          .filter { env =>
            env.protocolMessage.message.viewType == ViewType.TransferOutViewType
          }
      val size = transferOutResults.size
      if (size != 1)
        throw InvalidTransferOutResult(
          result.content,
          s"The deliver event must contain exactly one transfer-out result, but found $size.",
        )
      transferOutResults(0).protocolMessage.message
  }

  unwrap.verdict match {
    case _: Verdict.Approve => ()
    case _: Verdict.MediatorReject | _: Verdict.ParticipantReject =>
      throw InvalidTransferOutResult(result.content, "The transfer-out result must be approving.")
  }

  def transferId: TransferId = TransferId(SourceDomainId(unwrap.domainId), unwrap.requestId.unwrap)

  override def pretty: Pretty[DeliveredTransferOutResult] = prettyOfParam(_.unwrap)
}

object DeliveredTransferOutResult {

  final case class InvalidTransferOutResult(
      transferOutResult: RawProtocolEvent,
      message: String,
  ) extends RuntimeException(s"$message: $transferOutResult")

  def create(
      resultE: WithOpeningErrors[SignedContent[RawProtocolEvent]]
  ): Either[InvalidTransferOutResult, DeliveredTransferOutResult] =
    for {
      // The event signature would be invalid if some envelopes could not be opened upstream.
      // However, this should not happen, because transfer out messages are sent by the mediator,
      // who is trusted not to send bad envelopes.
      result <- Either.cond(
        resultE.hasNoErrors,
        resultE.event,
        InvalidTransferOutResult(
          resultE.event.content,
          "Result event contains envelopes that could not be deserialized.",
        ),
      )
      castToDeliver <- result
        .traverse(Deliver.fromSequencedEvent)
        .toRight(
          InvalidTransferOutResult(
            result.content,
            "Only a Deliver event contains a transfer-out result.",
          )
        )
      deliveredTransferOutResult <- Either.catchOnly[InvalidTransferOutResult] {
        DeliveredTransferOutResult(castToDeliver)
      }
    } yield deliveredTransferOutResult
}
