// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.protocol.{ExternalAuthorization, RequestId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

class AuthorizationValidator(participantId: ParticipantId, enableExternalAuthorization: Boolean)(
    implicit executionContext: ExecutionContext
) {

  def checkAuthorization(
      parsedRequest: ParsedTransactionRequest
  )(implicit traceContext: TraceContext): Future[Map[ViewPosition, String]] =
    checkAuthorization(
      parsedRequest.requestId,
      parsedRequest.rootViewTrees.forgetNE,
      parsedRequest.snapshot.ipsSnapshot,
    )

  def checkAuthorization(
      requestId: RequestId,
      rootViews: Seq[FullTransactionViewTree],
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[Map[ViewPosition, String]] =
    rootViews
      .parTraverseFilter { rootView =>
        val authorizers =
          rootView.viewParticipantData.rootAction.authorizers

        def err(details: String): String =
          show"Received a request with id $requestId with a view that is not correctly authorized. Rejecting request...\n$details"

        def noAuthorizationForParties(
            notCoveredBySubmittingParty: Set[LfPartyId]
        ): Future[Option[String]] =
          Future.successful(
            Some(
              err(
                show"Missing authorization for $notCoveredBySubmittingParty through the submitting parties."
              )
            )
          )

        def notAuthorizedBySubmittingParticipant(
            submittingParticipant: ParticipantId,
            notAllowedBySubmittingParticipant: Seq[LfPartyId],
        ): Option[String] =
          Some(
            err(
              show"The submitting participant $submittingParticipant is not authorized to submit on behalf of the submitting parties ${notAllowedBySubmittingParticipant.toSeq}."
            )
          )

        def missingExternalAuthorizers(
            parties: Set[LfPartyId]
        ): Future[Option[String]] =
          Future.successful(
            Some(
              err(
                show"An externally signed transaction is missing the following acting parties: $parties."
              )
            )
          )

        def missingValidFingerprints(
            parties: Set[PartyId]
        ): String =
          err(
            show"The following parties have provided fingerprints that are not valid: $parties"
          )

        def checkMetadata(submitterMetadata: SubmitterMetadata): Future[Option[String]] =
          submitterMetadata.externalAuthorization match {
            case None => checkNonExternallySignedMetadata(submitterMetadata)
            case Some(tx) if enableExternalAuthorization =>
              checkExternallySignedMetadata(submitterMetadata, tx)
            case Some(_) =>
              Future.successful(
                Some(
                  err(
                    "External authentication is not enabled (to enable set enable-external-authorization parameter)"
                  )
                )
              )
          }

        def checkNonExternallySignedMetadata(
            submitterMetadata: SubmitterMetadata
        ): Future[Option[String]] = {
          val notCoveredBySubmittingParty = authorizers -- submitterMetadata.actAs
          if (notCoveredBySubmittingParty.nonEmpty) {
            noAuthorizationForParties(notCoveredBySubmittingParty)
          } else {
            for {
              notAllowedBySubmittingParticipant <- snapshot.canNotSubmit(
                submitterMetadata.submittingParticipant,
                submitterMetadata.actAs.toSeq,
              )
            } yield
              if (notAllowedBySubmittingParticipant.nonEmpty) {
                notAuthorizedBySubmittingParticipant(
                  submitterMetadata.submittingParticipant,
                  notAllowedBySubmittingParticipant.toSeq,
                )
              } else None
          }
        }

        def checkExternallySignedMetadata(
            submitterMetadata: SubmitterMetadata,
            tx: ExternalAuthorization,
        ): Future[Option[String]] = {
          // The signed TX parties covers all act-as parties
          val signedAs = tx.signatures.keySet
          val missingAuthorizers = (submitterMetadata.actAs ++ authorizers) -- signedAs.map(_.toLf)
          if (missingAuthorizers.nonEmpty) {
            missingExternalAuthorizers(missingAuthorizers)
          } else
            {
              // The parties are external
              tx.signatures.toSeq
                .parTraverseFilter { case (p, s) =>
                  snapshot.partyAuthorization(p).map {
                    case Some(info) =>
                      val signedFingerprints = s.map(_.signedBy).toSet
                      val invalidFingerprints =
                        signedFingerprints -- info.signingKeys.map(_.fingerprint).toSet
                      if (invalidFingerprints.nonEmpty) {
                        Some(p)
                      } else {
                        Option.when(signedFingerprints.sizeIs < info.threshold.unwrap)(p)
                      }
                    case None =>
                      Some(p)
                  }
                }
                .map(_.toSet)
                .map { parties =>
                  Option.when(parties.nonEmpty)(missingValidFingerprints(parties))
                }
            }.failOnShutdownToAbortException(functionFullName)
        }

        def checkAuthorizersAreNotHosted(): Future[Option[String]] =
          snapshot.hostedOn(authorizers, participantId).map { hostedAuthorizers =>
            // If this participant hosts an authorizer, it should also have received the parent view.
            // As rootView is not a top-level (submitter metadata is blinded), there is a gap in the authorization chain.

            Option.when(hostedAuthorizers.nonEmpty)(
              err(
                show"Missing authorization for ${hostedAuthorizers.keys.toSeq.sorted}, ${rootView.viewPosition}."
              )
            )
          }

        (rootView.submitterMetadataO match {
          case Some(submitterMetadata) =>
            // The submitter metadata is unblinded -> rootView is a top-level view
            checkMetadata(submitterMetadata)

          case None =>
            // The submitter metadata is blinded -> rootView is not a top-level view
            checkAuthorizersAreNotHosted()
        })
          .map(_.map(rootView.viewPosition -> _))

      }
      .map(_.toMap)

}
