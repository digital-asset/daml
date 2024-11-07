// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  InteractiveSubmission,
  Signature,
}
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

class AuthenticationValidator(implicit
    executionContext: ExecutionContext
) {

  def verifyViewSignatures(
      parsedRequest: ParsedTransactionRequest
  )(implicit traceContext: TraceContext): Future[Map[ViewPosition, String]] = verifyViewSignatures(
    parsedRequest.requestId,
    parsedRequest.rootViewTreesWithSignatures.forgetNE,
    parsedRequest.snapshot,
  )

  private def verifyViewSignatures(
      requestId: RequestId,
      rootViews: Seq[(FullTransactionViewTree, Option[Signature])],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): Future[Map[ViewPosition, String]] = {

    def verifySignature(
        viewWithSignature: (FullTransactionViewTree, Option[Signature])
    )(implicit traceContext: TraceContext): Future[Option[(ViewPosition, String)]] = {

      val (view, signatureO) = viewWithSignature

      def err(details: String): String =
        show"Received a request with id $requestId with a view that is not correctly authenticated. Rejecting request...\n$details"

      view.submitterMetadataO match {
        // RootHash -> is a blinded tree
        case None => Future(None)
        // SubmitterMetadata -> information on the submitter of the tree
        case Some(submitterMetadata: SubmitterMetadata) =>
          signatureO match {
            case Some(signature) =>
              (for {
                // Verify the participant signature
                _ <- snapshot
                  .verifySignature(
                    view.rootHash.unwrap,
                    submitterMetadata.submittingParticipant,
                    signature,
                  )
                  .leftMap(_.show)

                // Verify the signature of any externally signed parties
                _ <- submitterMetadata.externalAuthorization
                  .fold(EitherT.pure[Future, String](())) { e =>
                    val hash = InteractiveSubmission.computeHashV1(submitterMetadata.commandId)
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
                      err(s"View ${view.viewPosition} has an invalid signature: $cause."),
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
                    err(s"View ${view.viewPosition} is missing a signature."),
                  )
                )
              )

          }
      }
    }

    for {
      signatureCheckErrors <- rootViews.parTraverseFilter(verifySignature)
    } yield signatureCheckErrors.toMap
  }
}
