// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult.extractConfirmationResultMessage
import com.digitalasset.canton.sequencing.RawProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  SignedContent,
  WithOpeningErrors,
}
import com.digitalasset.canton.util.ReassignmentTag.Source

/** Invariants:
  * - Deliver event contains exactly one protocol message with viewType == UnassignmentViewType
  * - Verdict of this event is Approve
  */
final case class DeliveredUnassignmentResult private (
    result: SignedContent[Deliver[DefaultOpenEnvelope]]
) extends PrettyPrinting {

  // Safe by the factory method
  val signedConfirmationResult: SignedProtocolMessage[ConfirmationResultMessage] =
    extractConfirmationResultMessage(result.content).valueOr(throw _)

  val unwrap: ConfirmationResultMessage = signedConfirmationResult.message

  def reassignmentId: ReassignmentId =
    ReassignmentId(Source(unwrap.domainId), unwrap.requestId.unwrap)

  override protected def pretty: Pretty[DeliveredUnassignmentResult] = prettyOfParam(_.unwrap)
}

object DeliveredUnassignmentResult {

  final case class InvalidUnassignmentResult(
      unassignmentResult: RawProtocolEvent,
      message: String,
  ) extends RuntimeException(s"$message: $unassignmentResult")

  private def extractConfirmationResultMessage(
      content: Deliver[DefaultOpenEnvelope]
  ): Either[InvalidUnassignmentResult, SignedProtocolMessage[ConfirmationResultMessage]] =
    content match {
      case Deliver(_, _, _, _, Batch(envelopes), _, _) =>
        val unassignmentResults =
          envelopes
            .mapFilter(
              ProtocolMessage.select[SignedProtocolMessage[ConfirmationResultMessage]]
            )
            .filter { env =>
              env.protocolMessage.message.viewType == ViewType.UnassignmentViewType
            }
        val unassignmentResultsCount = unassignmentResults.size
        val envelopesCount = envelopes.size

        for {
          _ <- Either.cond(
            unassignmentResultsCount == 1,
            (),
            InvalidUnassignmentResult(
              content,
              s"The deliver event must contain exactly one unassignment result, but found $unassignmentResultsCount.",
            ),
          )
          _ <- Either.cond(
            envelopesCount == 1,
            (),
            InvalidUnassignmentResult(
              content,
              s"The deliver event must contain exactly one envelope, but found $envelopesCount.",
            ),
          )
        } yield unassignmentResults(0).protocolMessage
    }

  /** - Deliver event contains exactly one protocol message with viewType == UnassignmentViewType
    * - Verdict of this event is Approve
    */
  private def checkInvariants(
      result: SignedContent[Deliver[DefaultOpenEnvelope]]
  ): Either[InvalidUnassignmentResult, Unit] = for {
    confirmationResultMessage <- extractConfirmationResultMessage(result.content)
    verdict = confirmationResultMessage.message.verdict
    _ <- Either.cond(
      verdict.isApprove,
      (),
      InvalidUnassignmentResult(
        result.content,
        s"The unassignment result must be approving; found: $verdict",
      ),
    )
  } yield ()

  def create(
      result: SignedContent[Deliver[DefaultOpenEnvelope]]
  ): Either[InvalidUnassignmentResult, DeliveredUnassignmentResult] =
    checkInvariants(result).map(_ => DeliveredUnassignmentResult(result))

  def create(
      resultE: WithOpeningErrors[SignedContent[RawProtocolEvent]]
  ): Either[InvalidUnassignmentResult, DeliveredUnassignmentResult] =
    for {
      // The event signature would be invalid if some envelopes could not be opened upstream.
      // However, this should not happen, because unassignment messages are sent by the mediator,
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

      _ <- checkInvariants(castToDeliver)

      deliveredUnassignmentResult <- Either.catchOnly[InvalidUnassignmentResult] {
        DeliveredUnassignmentResult(castToDeliver)
      }
    } yield deliveredUnassignmentResult
}
