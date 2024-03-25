// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.{FullTransactionViewTree, ViewPosition}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

class AuthorizationValidator(participantId: ParticipantId)(implicit
    executionContext: ExecutionContext
) {

  def checkAuthorization(
      requestId: RequestId,
      rootViews: NonEmpty[Seq[FullTransactionViewTree]],
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[Map[ViewPosition, String]] =
    rootViews.forgetNE
      .parTraverseFilter { rootView =>
        val authorizers =
          rootView.viewParticipantData.rootAction.authorizers

        def err(details: String): String =
          show"Received a request with id $requestId with a view that is not correctly authorized. Rejecting request...\n$details"

        val errOF = rootView.submitterMetadataO match {
          case Some(submitterMetadata) =>
            // The submitter metadata is unblinded -> rootView is a top-level view

            val notCoveredBySubmittingParty = authorizers -- submitterMetadata.actAs
            if (notCoveredBySubmittingParty.nonEmpty) {
              Future.successful(
                Some(
                  err(
                    show"Missing authorization for $notCoveredBySubmittingParty through the submitting parties."
                  )
                )
              )
            } else {
              for {
                notAllowedBySubmittingParticipant <- snapshot.canNotSubmit(
                  submitterMetadata.submittingParticipant,
                  submitterMetadata.actAs.toSeq,
                )
              } yield
                if (notAllowedBySubmittingParticipant.nonEmpty) {
                  Some(
                    err(
                      show"The submitting participant ${submitterMetadata.submittingParticipant} is not authorized to submit on behalf of the submitting parties ${notAllowedBySubmittingParticipant.toSeq}."
                    )
                  )
                } else None
            }
          case None =>
            // The submitter metadata is blinded -> rootView is not a top-level view
            snapshot.hostedOn(authorizers, participantId).map { hostedAuthorizers =>
              // If this participant hosts an authorizer, it should also have received the parent view.
              // As rootView is not a top-level (submitter metadata is blinded), there is a gap in the authorization chain.

              Option.when(hostedAuthorizers.nonEmpty)(
                err(
                  show"Missing authorization for ${hostedAuthorizers.keys.toSeq.sorted}, ${rootView.viewPosition}."
                )
              )
            }
        }

        errOF.map(_.map(rootView.viewPosition -> _))
      }
      .map(_.toMap)
}
