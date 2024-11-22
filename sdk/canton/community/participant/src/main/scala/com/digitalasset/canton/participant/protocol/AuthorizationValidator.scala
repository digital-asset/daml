// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{FullTransactionViewTree, SubmitterMetadata, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.ParsedTransactionRequest
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.ExecutionContext

class AuthorizationValidator(participantId: ParticipantId, enableExternalAuthorization: Boolean)(
    implicit executionContext: ExecutionContext
) {

  def checkAuthorization(
      parsedRequest: ParsedTransactionRequest
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    checkAuthorization(
      parsedRequest.requestId,
      parsedRequest.rootViewTrees.forgetNE,
      parsedRequest.snapshot.ipsSnapshot,
    )

  def checkAuthorization(
      requestId: RequestId,
      rootViews: Seq[FullTransactionViewTree],
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[ViewPosition, String]] =
    rootViews
      .parTraverseFilter { rootView =>
        val authorizers =
          rootView.viewParticipantData.rootAction.authorizers

        def err(details: String): String =
          show"Received a request with id $requestId with a view that is not correctly authorized. Rejecting request...\n$details"

        def noAuthorizationForParties(
            notCoveredBySubmittingParty: Set[LfPartyId]
        ): FutureUnlessShutdown[Option[String]] =
          FutureUnlessShutdown.pure(
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

        def checkMetadata(
            submitterMetadata: SubmitterMetadata
        ): FutureUnlessShutdown[Option[String]] =
          submitterMetadata.externalAuthorization match {
            case None => checkNonExternallySignedMetadata(submitterMetadata)
            case Some(_) if enableExternalAuthorization =>
              checkExternallySignedMetadata(submitterMetadata)
            case Some(_) =>
              FutureUnlessShutdown.pure(
                Some(
                  err(
                    "External authentication is not enabled (to enable set enable-external-authorization parameter)"
                  )
                )
              )
          }

        def checkNonExternallySignedMetadata(
            submitterMetadata: SubmitterMetadata
        ): FutureUnlessShutdown[Option[String]] = {
          val notCoveredBySubmittingParty = authorizers -- submitterMetadata.actAs
          if (notCoveredBySubmittingParty.nonEmpty) {
            noAuthorizationForParties(notCoveredBySubmittingParty)
          } else {
            for {
              notAllowedBySubmittingParticipant <- FutureUnlessShutdown.outcomeF(
                snapshot.canNotSubmit(
                  submitterMetadata.submittingParticipant,
                  submitterMetadata.actAs.toSeq,
                )
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
            submitterMetadata: SubmitterMetadata
        ): FutureUnlessShutdown[Option[String]] = {
          val notCoveredBySubmittingParty = authorizers -- submitterMetadata.actAs
          if (notCoveredBySubmittingParty.nonEmpty) {
            noAuthorizationForParties(notCoveredBySubmittingParty)
          } else {
            // Unlike for non-external submissions, here we don't check that the submitting participant
            // is allowed to submit on behalf of the actAs parties. That's because external parties indeed
            // do not need to be hosted with submission rights anywhere. We've already validated in the
            // AuthenticationValidator that the signatures provided cover the actAs parties, so as long as the
            // actAs parties also cover the authorizers of the transaction, it is enough.
            // If we later allow submissions with a mixed bag of external and non-external parties, we need to revisit this
            // check
            FutureUnlessShutdown.pure(None)
          }
        }

        def checkAuthorizersAreNotHosted(): FutureUnlessShutdown[Option[String]] =
          FutureUnlessShutdown.outcomeF(snapshot.hostedOn(authorizers, participantId)).map {
            hostedAuthorizers =>
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
