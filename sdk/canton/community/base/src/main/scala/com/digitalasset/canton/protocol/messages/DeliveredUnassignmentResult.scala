// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult.InvalidUnassignmentResult
import com.digitalasset.canton.protocol.{ReassignmentId, SourceDomainId}
import com.digitalasset.canton.sequencing.RawProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  SignedContent,
  WithOpeningErrors,
}

final case class DeliveredUnassignmentResult(result: SignedContent[Deliver[DefaultOpenEnvelope]])
    extends PrettyPrinting {

  val unwrap: ConfirmationResultMessage = result.content match {
    case Deliver(_, _, _, _, Batch(envelopes), _, _) =>
      val unassignmentResults =
        envelopes
          .mapFilter(env =>
            ProtocolMessage.select[SignedProtocolMessage[ConfirmationResultMessage]](env)
          )
          .filter { env =>
            env.protocolMessage.message.viewType == ViewType.UnassignmentViewType
          }
      val size = unassignmentResults.size
      if (size != 1)
        throw InvalidUnassignmentResult(
          result.content,
          s"The deliver event must contain exactly one unassignment result, but found $size.",
        )
      unassignmentResults(0).protocolMessage.message
  }

  unwrap.verdict match {
    case _: Verdict.Approve => ()
    case _: Verdict.MediatorReject | _: Verdict.ParticipantReject =>
      throw InvalidUnassignmentResult(result.content, "The unassignment result must be approving.")
  }

  def reassignmentId: ReassignmentId =
    ReassignmentId(SourceDomainId(unwrap.domainId), unwrap.requestId.unwrap)

  override def pretty: Pretty[DeliveredUnassignmentResult] = prettyOfParam(_.unwrap)
}

object DeliveredUnassignmentResult {

  final case class InvalidUnassignmentResult(
      unassignmentResult: RawProtocolEvent,
      message: String,
  ) extends RuntimeException(s"$message: $unassignmentResult")

  def create(
      resultE: WithOpeningErrors[SignedContent[RawProtocolEvent]]
  ): Either[InvalidUnassignmentResult, DeliveredUnassignmentResult] =
    for {
      // The event signature would be invalid if some envelopes could not be opened upstream.
      // However, this should not happen, because transfer out messages are sent by the mediator,
      // who is trusted not to send bad envelopes.
      result <- Either.cond(
        resultE.hasNoErrors,
        resultE.event,
        InvalidUnassignmentResult(
          resultE.event.content,
          "Result event contains envelopes that could not be deserialized.",
        ),
      )
      castToDeliver <- result
        .traverse(Deliver.fromSequencedEvent)
        .toRight(
          InvalidUnassignmentResult(
            result.content,
            "Only a Deliver event contains an unassignment result.",
          )
        )
      deliveredUnassignmentResult <- Either.catchOnly[InvalidUnassignmentResult] {
        DeliveredUnassignmentResult(castToDeliver)
      }
    } yield deliveredUnassignmentResult
}
