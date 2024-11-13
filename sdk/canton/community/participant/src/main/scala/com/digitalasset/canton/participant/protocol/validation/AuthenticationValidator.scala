// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.CommandId
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  InteractiveSubmission,
  Signature,
}
import com.digitalasset.canton.data.{FullReassignmentViewTree, ViewPosition, ViewTree}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ParsedReassignmentRequest
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

private[protocol] object AuthenticationValidator {
  def verifyViewSignatures(
      parsedRequest: ParsedTransactionRequest
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Map[ViewPosition, AuthenticationError]] =
    parsedRequest.rootViewTreesWithSignatures.forgetNE
      .parTraverseFilter { case (rootView, signatureO) =>
        verifySignature(
          requestId = parsedRequest.requestId,
          snapshot = parsedRequest.snapshot,
          view = rootView,
          signatureO = signatureO,
          submittingParticipantO = rootView.submitterMetadataO.map(_.submittingParticipant),
          externalAuthorizationO = rootView.submitterMetadataO.flatMap(submitterMetadata =>
            submitterMetadata.externalAuthorization.map(_ -> submitterMetadata.commandId)
          ),
        )
      }
      .map(_.toMap)

  def verifyViewSignature[VT <: FullReassignmentViewTree](parsed: ParsedReassignmentRequest[VT])(
      implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Option[AuthenticationError]] =
    verifySignature(
      parsed.requestId,
      parsed.snapshot,
      parsed.fullViewTree,
      parsed.signatureO,
      Some(parsed.fullViewTree.submitterMetadata.submittingParticipant),
      externalAuthorizationO = None,
    ).map(_.map { case (_, error) => error })

  private def verifySignature(
      requestId: RequestId,
      snapshot: DomainSnapshotSyncCryptoApi,
      view: ViewTree,
      signatureO: Option[Signature],
      submittingParticipantO: Option[ParticipantId],
      externalAuthorizationO: Option[(ExternalAuthorization, CommandId)],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Option[(ViewPosition, AuthenticationError)]] =
    submittingParticipantO match {
      case None => Future(None)
      case Some(submittingParticipant) =>
        signatureO match {
          case Some(signature) =>
            (for {
              // check for an invalid signature
              _ <- snapshot
                .verifySignature(
                  view.rootHash.unwrap,
                  submittingParticipant,
                  signature,
                )
                .leftMap(_.show)

              // Verify the signature of any externally signed parties
              _ <- externalAuthorizationO
                .fold(EitherT.pure[Future, String](())) { case (e, commandId) =>
                  val hash = InteractiveSubmission.computeHashV1(commandId)
                  InteractiveSubmission
                    .verifySignatures(hash, e.signatures, snapshot)
                    .map(_ => ())
                    .failOnShutdownToAbortException("verifySignature")
                }
            } yield ()).fold(
              cause =>
                Some(
                  (
                    view.viewPosition,
                    AuthenticationError.InvalidSignature(
                      requestId,
                      view.viewPosition,
                      cause,
                    ),
                  )
                ),
              _ => None,
            )

          case None =>
            // the signature is missing
            Future(
              Some(
                (
                  view.viewPosition,
                  AuthenticationError.MissingSignature(
                    requestId,
                    view.viewPosition,
                  ),
                )
              )
            )
        }
    }
}

sealed trait AuthenticationError {
  def message: String
}

object AuthenticationError {
  final case class InvalidSignature(
      requestId: RequestId,
      viewPosition: ViewPosition,
      cause: String,
  ) extends AuthenticationError {
    override def message: String =
      err(requestId, s"View $viewPosition has an invalid signature: $cause.")
  }
  final case class MissingSignature(
      requestId: RequestId,
      viewPosition: ViewPosition,
  ) extends AuthenticationError {
    override def message: String =
      err(requestId, s"View $viewPosition is missing a signature.")
  }

  private def err(requestId: RequestId, details: String): String =
    show"Received a request with id $requestId with a view that is not correctly authenticated. Rejecting request...\n$details"
}
