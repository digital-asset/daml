// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.data.{ViewPosition, ViewTree}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.WrongRecipients
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.sequencing.protocol.{Recipient, Recipients}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, IterableUtil}

import scala.concurrent.{ExecutionContext, Future}

class RecipientsValidator[I](
    viewOfInput: I => ViewTree,
    recipientsOfInput: I => Recipients,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  import RecipientsValidator.*

  /** Checks the recipients of all inputs and discards inputs corresponding to views with invalid recipients.
    * Also reports a security alert on invalid recipients.
    *
    * Effectively, the method tries to establish consensus on whether the recipients of an input are valid, and
    * if consensus cannot be established, then the input is discarded.
    * So an input may even be discarded, if its recipients are valid (but not every recipient knows about this).
    *
    * A view v will be kept iff there is a path rp through the recipients tree (ordered leaf to root)
    * such that the following conditions hold:
    * 1. Every informee of the view is hosted by an active participant.
    * 2. Every informee participant of the view v is declared as a recipient of v in the first element of rp.
    * 3. For every descendant v2 of v and every informee participant p of v, the participant p is declared as recipient of v2.
    *    Thereby, if v2 and v have distance d, then participant p needs to be declared at element d+1 in rp.
    * 4. Every descendant of v also meets Conditions 1-3 with the same path rp.
    *
    * Why does this give us transparency?
    * If an informee participant p1 keeps a view v, then:
    * - Every informee of the view v is hosted by an active participant.
    * - Every informee participant of v has received v.
    * - Every informee participant of v has received every descendant of v.
    * - Every informee participant of v can evaluate the above conditions 1-4 for v and will conclude that v should be kept.
    *
    * @return inputs with valid recipients
    * @throws java.lang.IllegalArgumentException if the views corresponding to inputs have different root hashes
    */
  def retainInputsWithValidRecipients(
      requestId: RequestId,
      inputs: Seq[I],
      snapshot: PartyTopologySnapshotClient,
  )(implicit traceContext: TraceContext): Future[(Seq[WrongRecipients], Seq[I])] = {

    val rootHashes = inputs.map(viewOfInput(_).rootHash).distinct
    ErrorUtil.requireArgument(
      rootHashes.sizeCompare(1) <= 0,
      s"Views with different root hashes are not supported: $rootHashes",
    )

    for {
      informeeParticipantsOfPositionAndParty <-
        computeInformeeParticipantsOfPositionAndParty(inputs, snapshot)

    } yield {
      val context =
        Context(requestId, informeeParticipantsOfPositionAndParty)

      // Check Condition 1, i.e., detect inputs where the view has an informee without an active participant
      val inactivePartyPositions = computeInactivePartyPositions(context)

      // This checks Condition 2 and 3.
      val invalidRecipientPositions =
        inputs.mapFilter(input =>
          checkRecipientsTree(context, viewOfInput(input).viewPosition, recipientsOfInput(input))
        )

      val badViewPositions = invalidRecipientPositions ++ inactivePartyPositions

      // Check Condition 4, i.e., remove inputs that have a bad view position as descendant.
      inputs.map { input =>
        val viewTree = viewOfInput(input)

        val isGood = badViewPositions.forall(badViewPosition =>
          !ViewPosition.isDescendant(badViewPosition, viewTree.viewPosition)
        )

        Either.cond(
          isGood,
          input,
          WrongRecipients(viewTree),
        )
      }.separate
    }
  }

  /** Yields the informeeParticipants of views, grouped by view position and party.
    * Filters out (with a security alert) any view that has an informee that is not hosted by an active participant.
    */
  private def computeInformeeParticipantsOfPositionAndParty(
      inputs: Seq[I],
      snapshot: PartyTopologySnapshotClient,
  ): Future[Map[List[MerklePathElement], Map[LfPartyId, Set[ParticipantId]]]] =
    inputs
      .parTraverse { input =>
        val view = viewOfInput(input)
        snapshot
          .activeParticipantsOfParties(
            view.informees.map(_.party).toList
          )
          .map(view.viewPosition.position -> _)
      }
      // It is ok to remove duplicates, as the informees of a view depend only on view.viewPosition.
      // Here, we make use of the assumption that all views have the same root hash.
      .map(_.toMap)

  /** Yields the positions of those views that have an informee without an active participant.
    */
  private def computeInactivePartyPositions(
      context: Context
  )(implicit traceContext: TraceContext): Seq[ViewPosition] = {
    val Context(requestId, informeeParticipantsOfPositionAndParty) = context

    informeeParticipantsOfPositionAndParty.toSeq.mapFilter {
      case (viewPositionSeq, informeeParticipantsOfParty) =>
        val viewPosition = ViewPosition(viewPositionSeq)

        val inactiveParties =
          informeeParticipantsOfParty.collect {
            case (party, participants) if participants.isEmpty => party
          }.toSet

        Option.when(inactiveParties.nonEmpty) {
          SyncServiceAlarm
            .Warn(
              s"Received a request with id $requestId where the view at $viewPosition has " +
                s"informees without an active participant: $inactiveParties. " +
                s"Discarding $viewPosition..."
            )
            .report()

          viewPosition
        }
    }
  }

  /** Yields the closest (i.e. bottom-most) ancestor of a view (if any)
    * that needs to be discarded due to incorrect recipients in `recipients`.
    * Also reports a security alert on any detected violation of transparency or privacy.
    *
    * A view needs to be discarded if and only if it needs to be discarded due to incorrect recipients on all paths in `recipients`.
    * Conversely, if a view has correct recipients on a single path in `recipients`, it should be kept.
    *
    * @param context precomputed information
    * @param mainViewPosition the method will validate this view position as well as all ancestor positions
    * @param recipients the declared recipients for the view at mainViewPosition
    * @return the position of the view to be discarded (if any)
    */
  private def checkRecipientsTree(
      context: Context,
      mainViewPosition: ViewPosition,
      recipients: Recipients,
  )(implicit
      traceContext: TraceContext
  ): Option[ViewPosition] = {

    val allRecipientPathsViewToRoot = recipients.allPaths.map(_.reverse)

    if (allRecipientPathsViewToRoot.sizeCompare(1) > 0) {
      SyncServiceAlarm
        .Warn(
          s"Received a request with id ${context.requestId} where the view at $mainViewPosition has a non-linear recipients tree. " +
            s"Processing all paths of the tree.\n$recipients"
        )
        .report()
    }

    val badViewPositions = allRecipientPathsViewToRoot
      .map(checkRecipientsPath(context, recipients, mainViewPosition.position, _))

    val res = badViewPositions.minBy1 {
      case Some((viewPosition, _)) => viewPosition.position.size
      case None => 0
    }
    res.map { case (viewPosition, alarm) =>
      alarm.report()
      viewPosition
    }
  }

  /** Yields the closest (i.e. bottom-most) ancestor of a view (if any)
    * that needs to be discarded due to incorrect recipients in `recipientsPathViewToRoot`.
    * Also reports a security alert on any detected violation of privacy.
    *
    * @param context precomputed information
    * @param mainRecipients used for error messages only
    * @param mainViewPosition the method will validate this view position as well as all ancestor positions
    * @param recipientsPathViewToRoot the declared recipients of the view at `mainViewPosition` ordered view to root;
    *                                 so the order is the same as in the elements of `mainViewPosition`.
    * @return the position of the view to be discarded (if any) together with the corresponding reason (as security alert)
    */
  private def checkRecipientsPath(
      context: Context,
      mainRecipients: Recipients,
      mainViewPosition: List[MerklePathElement],
      recipientsPathViewToRoot: Seq[Set[Recipient]],
  )(implicit traceContext: TraceContext): Option[(ViewPosition, SyncServiceAlarm.Warn)] = {
    val Context(requestId, informeeParticipantsOfPositionAndParty) =
      context

    IterableUtil
      .zipAllOption(
        recipientsPathViewToRoot,
        mainViewPosition.tails.iterator.to(Iterable): Iterable[List[MerklePathElement]],
      )
      .view // lazy evaluation, so computation stops after the first match
      .map {
        case (None, None) =>
          ErrorUtil.invalidState("zipAll has inserted the default element on both sides.")

        case (Some(_recipientGroup), None) =>
          // recipientsPathViewToRoot is too long. This is not a problem for transparency, but it can be a problem for privacy.
          SyncServiceAlarm
            .Warn(
              s"Received a request with id $requestId where the view at $mainViewPosition has too many levels of recipients. Continue processing...\n$mainRecipients"
            )
            .report()
          None

        case (None, Some(viewPosition)) =>
          if (informeeParticipantsOfPositionAndParty.contains(viewPosition)) {
            // If we receive a view, we also need to receive corresponding recipient groups for the view and all descendants.
            // This is not the case here, so we need to discard the view at viewPosition.

            val alarm = SyncServiceAlarm
              .Warn(
                s"Received a request with id $requestId where the view at $mainViewPosition has " +
                  s"no recipients group for $viewPosition. " +
                  s"Discarding $viewPosition with all ancestors..."
              )

            Some(ViewPosition(viewPosition) -> alarm)

          } else {
            // We have not received the view at viewPosition. So there is no point in discarding it.
            // If we should have received the view at viewPosition, the other honest recipients will discard it.
            None
          }

        case (Some(_), Some(viewPosition))
            if !informeeParticipantsOfPositionAndParty.contains(viewPosition) =>
          // Since there is a recipient group, either the current recipient group or
          // an ancestor thereof contains this participant as recipient. (Property guaranteed by the sequencer.)
          // Hence, we should have received the current view.
          // Alarm, as this is not the case.

          val alarm = SyncServiceAlarm
            .Warn(
              s"Received a request with id $requestId where the view at $viewPosition is missing. " +
                s"Discarding all ancestors of $viewPosition..."
            )

          Some(ViewPosition(viewPosition) -> alarm)

        case (Some(recipientGroup), Some(viewPosition)) =>
          // We have received a view and there is a recipient group.
          // The view at viewPosition should be kept if the recipient group contains all informee participants of the view.

          val informeeParticipantsOfParty =
            informeeParticipantsOfPositionAndParty(viewPosition)

          val informeeRecipients = informeeParticipantsOfParty.toList
            .flatMap { case (_party, participants) =>
              participants.map(Recipient(_))
            }
            .toSet[Recipient]

          val extraRecipients = recipientGroup -- informeeRecipients
          if (extraRecipients.nonEmpty)
            SyncServiceAlarm
              .Warn(
                s"Received a request with id $requestId where the view at $mainViewPosition has " +
                  s"extra recipients $extraRecipients for the view at $viewPosition. " +
                  s"Continue processing..."
              )
              .report()

          val missingRecipients = informeeRecipients -- recipientGroup
          lazy val missingRecipientsAlarm = SyncServiceAlarm
            .Warn(
              s"Received a request with id $requestId where the view at $mainViewPosition has " +
                s"missing recipients $missingRecipients for the view at $viewPosition. " +
                s"Discarding $viewPosition with all ancestors..."
            )
          Option.when(missingRecipients.nonEmpty)(
            ViewPosition(viewPosition) -> missingRecipientsAlarm
          )
      }
      .collectFirst { case Some(result) => result }
  }
}

object RecipientsValidator {

  private final case class Context(
      requestId: RequestId,
      informeeParticipantsOfPositionAndParty: Map[
        List[MerklePathElement],
        Map[LfPartyId, Set[ParticipantId]],
      ],
  )
}
