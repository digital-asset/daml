// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{
  Hash,
  InteractiveSubmission,
  Signature,
  SigningKeyUsage,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{
  FullReassignmentViewTree,
  FullTransactionViewTree,
  SubmitterMetadata,
  ViewPosition,
  ViewTree,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ParsedReassignmentRequest
import com.digitalasset.canton.participant.protocol.validation.AuthenticationError.{
  FailedToComputeExternallySignedHash,
  InvalidSignature,
  MissingTopLevelView,
  MultipleExternallySignedRootViews,
}
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.{
  LazyAsyncReInterpretation,
  LazyAsyncReInterpretationMap,
}
import com.digitalasset.canton.participant.util.DAMLe.{ContractEnricher, TransactionEnricher}
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

private[protocol] object AuthenticationValidator {

  /** @param viewAuthenticationErrors
    *   authentication errors by view position
    * @param externalHash
    *   if the transaction has been signed externally and the the signature has been authenticated
    *   successfully, contains the transaction hash of the transaction. Errors including signature
    *   validation errors are returned as part of viewAuthenticationErrors.
    */
  final case class AuthenticationValidatorResult(
      viewAuthenticationErrors: Map[ViewPosition, AuthenticationError],
      externalHash: Option[Hash],
  )

  def verifyViewSignatures(
      parsedRequest: ParsedTransactionRequest,
      reInterpretedTopLevelViewsEval: LazyAsyncReInterpretationMap,
      synchronizerId: PhysicalSynchronizerId,
      transactionEnricher: TransactionEnricher,
      createNodeEnricher: ContractEnricher,
      logger: TracedLogger,
      messagePayloadLoggingEnabled: Boolean,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[AuthenticationValidatorResult] = {

    // Verify participant signature on the root view
    def verifyParticipantSignatureForRootView(
        rootView: FullTransactionViewTree,
        signatureO: Option[Signature],
        submittingParticipant: ParticipantId,
    ): FutureUnlessShutdown[Option[(ViewPosition, AuthenticationError)]] =
      verifyParticipantSignature(
        requestId = parsedRequest.requestId,
        snapshot = parsedRequest.snapshot,
        view = rootView,
        signatureO = signatureO,
        submittingParticipant = submittingParticipant,
      )

    // Verify external party signature on the root view (if provided), and returns transaction hash
    def verifyExternalPartySignatureForRootView(
        rootView: FullTransactionViewTree,
        submitterMetadata: SubmitterMetadata,
    ): FutureUnlessShutdown[Either[AuthenticationError, Option[Hash]]] =
      reInterpretedTopLevelViewsEval.get(rootView.viewHash) match {
        case Some(reInterpretationET) =>
          verifyExternalPartySignature(
            viewTree = rootView,
            submitterMetadata = submitterMetadata,
            topology = parsedRequest.snapshot,
            protocolVersion = synchronizerId.protocolVersion,
            reInterpretationET = reInterpretationET,
            requestId = parsedRequest.requestId,
            synchronizerId = synchronizerId,
            transactionEnricher = transactionEnricher,
            createNodeEnricher = createNodeEnricher,
            logger = logger,
            messagePayloadLoggingEnabled = messagePayloadLoggingEnabled,
          )
        case None =>
          // If we don't have the re-interpreted transaction for this view it's either a programming error
          // (we didn't interpret all available roots in reInterpretedTopLevelViews, or we're missing the top level view entirely
          // despite having the submitterMetadata, which is also wrong
          FutureUnlessShutdown.pure(
            Left(
              MissingTopLevelView(parsedRequest.requestId)
            )
          )
      }

    // Verify participant and external party signatures on all root views
    def verifyRootViewSignatures =
      parsedRequest.rootViewTreesWithSignatures.forgetNE.parTraverse {
        case (rootView, signatureO) =>
          rootView.submitterMetadataO match {
            // RootHash -> is a blinded tree
            case None => FutureUnlessShutdown.pure(Right(None))
            case Some(submitterMetadata) =>
              for {
                participantSignatureError <- verifyParticipantSignatureForRootView(
                  rootView,
                  signatureO,
                  submitterMetadata.submittingParticipant,
                )
                externalSignatureValidation <- verifyExternalPartySignatureForRootView(
                  rootView,
                  submitterMetadata,
                )
              } yield {
                participantSignatureError match {
                  case Some(error) => error.asLeft[Option[(ViewPosition, Hash)]]
                  case None =>
                    // Attach the corresponding view position both in case of error and external hash
                    externalSignatureValidation.bimap(
                      rootView.viewPosition -> _,
                      _.map(rootView.viewPosition -> _),
                    )
                }
              }
          }
      }

    for {
      verifiedSignatures <- verifyRootViewSignatures
      (errors, externalHashesO) = verifiedSignatures.toList.separate
    } yield {
      val externalHashes = externalHashesO.flatten
      // We don't support multiple externally signed root views
      val (additionalErrors, externalHash) = externalHashes match {
        case Nil => (List.empty, None)
        // We only support a single external transaction hash and only if there is a single top level view
        case (_, hash) :: Nil if reInterpretedTopLevelViewsEval.sizeIs == 1 =>
          (List.empty, Some(hash))
        // A single hash with multiple top level views is an error
        case (viewPosition, _) :: Nil =>
          (
            List(
              (
                viewPosition,
                MultipleExternallySignedRootViews(
                  parsedRequest.requestId,
                  reInterpretedTopLevelViewsEval.size,
                ): AuthenticationError,
              )
            ),
            None,
          )
        // Multiple hashes is also an error
        case multipleHashes =>
          (
            multipleHashes.map { case (viewPosition, _) =>
              (
                viewPosition,
                MultipleExternallySignedRootViews(
                  parsedRequest.requestId,
                  externalHashes.size,
                ): AuthenticationError,
              )
            },
            None,
          )
      }
      AuthenticationValidatorResult(errors.toMap ++ additionalErrors.toMap, externalHash)
    }
  }

  def verifyViewSignature[VT <: FullReassignmentViewTree](parsed: ParsedReassignmentRequest[VT])(
      implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[AuthenticationError]] =
    verifyParticipantSignature(
      parsed.requestId,
      parsed.snapshot,
      parsed.fullViewTree,
      parsed.signatureO,
      parsed.fullViewTree.submitterMetadata.submittingParticipant,
    ).map(_.map { case (_, error) => error })

  private def verifyParticipantSignature(
      requestId: RequestId,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      view: ViewTree,
      signatureO: Option[Signature],
      submittingParticipant: ParticipantId,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Option[(ViewPosition, AuthenticationError)]] =
    signatureO match {
      case Some(signature) =>
        (for {
          // check for an invalid signature
          _ <- snapshot
            .verifySignature(
              view.rootHash.unwrap,
              submittingParticipant,
              signature,
              SigningKeyUsage.ProtocolOnly,
            )
            .leftMap(_.show)
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
        FutureUnlessShutdown.pure(
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

  // Checks that the provided external signatures are valid for the transaction
  private def verifyExternalPartySignature(
      viewTree: FullTransactionViewTree,
      submitterMetadata: SubmitterMetadata,
      topology: SynchronizerSnapshotSyncCryptoApi,
      protocolVersion: ProtocolVersion,
      reInterpretationET: LazyAsyncReInterpretation,
      synchronizerId: PhysicalSynchronizerId,
      transactionEnricher: TransactionEnricher,
      createNodeEnricher: ContractEnricher,
      requestId: RequestId,
      logger: TracedLogger,
      messagePayloadLoggingEnabled: Boolean,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Either[AuthenticationError, Option[Hash]]] = {
    // Re-compute the hash from the re-interpreted transaction and necessary metadata, and verify the signature
    def computeHashAndVerifyExternalSignature(
        externalAuthorization: ExternalAuthorization
    ): FutureUnlessShutdown[Either[AuthenticationError, Option[Hash]]] =
      // At this point we have to run interpretation on the view to get the necessary data to re-compute the hash
      reInterpretationET.value.value.flatMap {
        case Left(error) =>
          FutureUnlessShutdown.pure(
            Left(
              FailedToComputeExternallySignedHash(
                requestId,
                s"Failed to re-interpret transaction in order to compute externally signed hash: $error",
              )
            )
          )
        case Right(reInterpretedTopLevelView) =>
          // The trace contains detailed information about the transaction and is expensive to compute
          // Only compute it if message payload logging is enabled and in debug level
          val hashTracer =
            if (messagePayloadLoggingEnabled && logger.underlying.isDebugEnabled) {
              Some(HashTracer.StringHashTracer(traceSubNodes = true))
            } else None

          reInterpretedTopLevelView
            .computeHash(
              externalAuthorization.hashingSchemeVersion,
              submitterMetadata.actAs,
              submitterMetadata.commandId.unwrap,
              viewTree.transactionUuid,
              viewTree.mediator.group.value,
              synchronizerId.logical,
              protocolVersion,
              transactionEnricher,
              createNodeEnricher,
              hashTracer.getOrElse[HashTracer](HashTracer.NoOp),
            )
            // If Hash computation is successful, verify the signature is valid
            .flatMap { hash =>
              EitherT.liftF[FutureUnlessShutdown, String, Either[String, Option[Hash]]](
                verifyExternalSignaturesForActAs(
                  hash,
                  externalAuthorization,
                  submitterMetadata.actAs,
                ).map {
                  case error @ Some(_) =>
                    hashTracer.map(_.result).foreach { trace =>
                      logger.debug("Transaction hash computation trace:\n" + trace)
                    }
                    error
                  case None => None
                }.map(_.toLeft(Some(hash)))
              )
            }
            .map(res =>
              res.leftMap[AuthenticationError](signatureError =>
                InvalidSignature(
                  requestId,
                  viewTree.viewPosition,
                  signatureError,
                )
              )
            )
            // If we couldn't compute the hash, fail
            .valueOr(error =>
              Left(
                FailedToComputeExternallySignedHash(
                  requestId,
                  error,
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
          topology.pureCrypto,
          topology.ipsSnapshot,
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
      case Some(externalAuthorization) =>
        // If external signatures are provided, we verify they are valid and cover all the actAs parties
        computeHashAndVerifyExternalSignature(externalAuthorization)
      // If no external signatures are provided, there's nothing to verify here, and it means
      // this is a classic submission. The classic submission requirements then apply and will be checked
      // in the AuthorizationValidator (typically that the submitting participant must have submission rights
      // for the actAs parties)
      case None => FutureUnlessShutdown.pure(Right(None))
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
  final case class MultipleExternallySignedRootViews(
      requestId: RequestId,
      numberOfViews: Int,
  ) extends AuthenticationError {
    override def message: String =
      err(
        requestId,
        s"Only single root transactions can currently be externally signed. Got $numberOfViews",
      )
  }
  final case class FailedToComputeExternallySignedHash(
      requestId: RequestId,
      cause: String,
  ) extends AuthenticationError {
    override def message: String =
      err(requestId, s"Failed to compute externally signed hash: $cause")
  }
  final case class MissingTopLevelView(
      requestId: RequestId
  ) extends AuthenticationError {
    override def message: String =
      err(requestId, "Missing top level view to validate external signature")
  }

  private def err(requestId: RequestId, details: String): String =
    show"Received a request with id $requestId with a view that is not correctly authenticated. Rejecting request...\n$details"
}
