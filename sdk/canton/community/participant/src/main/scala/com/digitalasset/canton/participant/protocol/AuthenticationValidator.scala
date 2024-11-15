// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  Hash,
  InteractiveSubmission,
  Signature,
}
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.LazyAsyncReInterpretation
import com.digitalasset.canton.participant.util.DAMLe.TransactionEnricher
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AuthenticationValidator(
    override val loggerFactory: NamedLoggerFactory,
    transactionEnricher: TransactionEnricher,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  private[protocol] def verifyViewSignatures(
      parsedRequest: ParsedTransactionRequest,
      reInterpretedTopLevelViewsEval: LazyAsyncReInterpretation,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    verifyViewSignatures(
      parsedRequest.requestId,
      parsedRequest.rootViewTreesWithSignatures.forgetNE,
      parsedRequest.snapshot,
      reInterpretedTopLevelViewsEval,
      domainId,
      protocolVersion,
    )

  private def verifyViewSignatures(
      requestId: RequestId,
      rootViews: Seq[(FullTransactionViewTree, Option[Signature])],
      snapshot: DomainSnapshotSyncCryptoApi,
      reInterpretedTopLevelViews: LazyAsyncReInterpretation,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] = {
    def err(details: String): String =
      show"Received a request with id $requestId with a view that is not correctly authenticated. Rejecting request...\n$details"

    def verifySignatures(
        viewWithSignature: (FullTransactionViewTree, Option[Signature])
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] = {

      val (view, signatureO) = viewWithSignature

      view.submitterMetadataO match {
        // RootHash -> is a blinded tree
        case None => FutureUnlessShutdown.pure(None)
        // SubmitterMetadata -> information on the submitter of the tree
        case Some(submitterMetadata: SubmitterMetadata) =>
          for {
            participantSignatureCheck <- verifyParticipantSignature(
              view,
              signatureO,
              submitterMetadata,
            )
            externalSignatureCheck <- verifyExternalPartySignature(
              view,
              submitterMetadata,
              snapshot,
              protocolVersion,
            )
          } yield participantSignatureCheck.orElse(externalSignatureCheck)
      }
    }

    def verifyParticipantSignature(
        view: FullTransactionViewTree,
        signatureO: Option[Signature],
        submitterMetadata: SubmitterMetadata,
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] =
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
              .mapK(FutureUnlessShutdown.outcomeK)
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
          FutureUnlessShutdown.pure(
            Some(
              (
                view.viewPosition,
                err(s"View ${view.viewPosition} is missing a signature."),
              )
            )
          )

      }

    // Checks that the provided external signatures are valid for the transaction
    def verifyExternalPartySignature(
        viewTree: FullTransactionViewTree,
        submitterMetadata: SubmitterMetadata,
        topology: DomainSnapshotSyncCryptoApi,
        protocolVersion: ProtocolVersion,
    ): FutureUnlessShutdown[Option[(ViewPosition, String)]] = {
      // Re-compute the hash from the re-interpreted transaction and necessary metadata, and verify the signature
      def computeHashAndVerifyExternalSignature(
          externalAuthorization: ExternalAuthorization
      ): FutureUnlessShutdown[Option[(ViewPosition, String)]] =
        reInterpretedTopLevelViews.get(viewTree.view.viewHash) match {
          case Some(reInterpretationET) =>
            // At this point we have to run interpretation on the view to get the necessary data to re-compute the hash
            reInterpretationET.value.value.flatMap {
              case Left(error) =>
                FutureUnlessShutdown.pure(
                  Some(
                    viewTree.viewPosition -> err(
                      s"Failed to re-interpret transaction in order to compute externally signed hash: $error"
                    )
                  )
                )
              case Right(reInterpretedTopLevelView) =>
                reInterpretedTopLevelView
                  .computeHash(
                    externalAuthorization.hashingSchemeVersion,
                    submitterMetadata.actAs,
                    submitterMetadata.commandId.unwrap,
                    viewTree.transactionUuid,
                    viewTree.mediator.group.value,
                    domainId,
                    protocolVersion,
                    transactionEnricher,
                  )
                  // If Hash computation is successful, verify the signature is valid
                  .flatMap { hash =>
                    EitherT.liftF[FutureUnlessShutdown, String, Option[String]](
                      verifyExternalSignaturesForActAs(
                        hash,
                        externalAuthorization,
                        submitterMetadata.actAs,
                      )
                    )
                  }
                  .map(
                    _.map(signatureError => viewTree.viewPosition -> err(signatureError))
                  )
                  // If we couldn't compute the hash, fail
                  .valueOr(error =>
                    Some(
                      viewTree.viewPosition -> err(
                        s"Failed to compute externally signed hash: $error"
                      )
                    )
                  )
            }
          case None =>
            // If we don't have the re-interpreted transaction for this view it's either a programming error
            // (we didn't interpret all available roots in reInterpretedTopLevelViews, or we're missing the top level view entirely
            // despite having the submitterMetadata, which is also wrong
            FutureUnlessShutdown.pure(
              Some(
                viewTree.viewPosition -> err(
                  "Missing top level view to validate external signature"
                )
              )
            )
        }

      // Verify the signatures provided by the act as parties are valid.
      // This proves the request really comes from the actAs parties.
      // Returns signature validation errors in the form of Some(errorString)
      def verifyExternalSignaturesForActAs(
          hash: Hash,
          externalAuthorization: ExternalAuthorization,
          actAs: NonEmpty[Set[LfPartyId]],
      ): FutureUnlessShutdown[Option[String]] =
        InteractiveSubmission
          .verifySignatures(
            hash,
            externalAuthorization.signatures,
            topology,
            actAs.forgetNE,
            logger,
          )
          .value
          .map {
            // Convert signature validation errors to a Some, as this is how we indicate failures
            case Left(error) => Some(error)
            // A valid signature verification translates to a None (absence of error)
            case Right(_) => None
          }

      submitterMetadata.externalAuthorization match {
        case Some(_) if reInterpretedTopLevelViews.size > 1 =>
          FutureUnlessShutdown.pure(
            Some(
              viewTree.viewPosition -> err(
                s"Only single root transactions can currently be externally signed"
              )
            )
          )
        case Some(externalAuthorization) =>
          // If external signatures are provided, we verify they are valid and cover all the actAs parties
          computeHashAndVerifyExternalSignature(externalAuthorization)
        // If no external signatures are provided, there's nothing to verify here, and it means
        // this is a classic submission. The classic submission requirements then apply and will be checked
        // in the AuthorizationValidator (typically that the submitting participant must have submission rights
        // for the actAs parties)
        case None => FutureUnlessShutdown.pure(None)
      }
    }

    for {
      signatureCheckErrors <- rootViews.parTraverseFilter(verifySignatures)
    } yield signatureCheckErrors.toMap
  }
}
