// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.OptionT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, Informee, ViewPosition}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import pprint.Tree

import scala.concurrent.{ExecutionContext, Future}

trait ResponseAggregator extends HasLoggerName with Product with Serializable {

  /** Type used to uniquely identify a view.
    */
  type VKey

  /** The request id of the [[com.digitalasset.canton.protocol.messages.InformeeMessage]]
    */
  def requestId: RequestId

  def request: MediatorConfirmationRequest

  /** The sequencer timestamp of the most recent message that affected this [[ResponseAggregator]]
    */
  def version: CantonTimestamp

  def isFinalized: Boolean

  /** Validate the additional confirmation response received and record if unless already finalized.
    */
  def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: ConfirmationResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregation[VKey]]]

  protected def validateResponse[VKEY: ViewKey](
      viewKeyO: Option[VKEY],
      rootHashO: Option[RootHash],
      responseTimestamp: CantonTimestamp,
      sender: ParticipantId,
      localVerdict: LocalVerdict,
      topologySnapshot: TopologySnapshot,
      confirmingParties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): OptionT[Future, List[(VKEY, Set[LfPartyId])]] = {
    implicit val tc = loggingContext.traceContext
    def authorizedPartiesOfSender(
        viewKey: VKEY,
        declaredConfirmingParties: Set[ConfirmingParty],
    ): OptionT[Future, Set[LfPartyId]] =
      localVerdict match {
        case malformed: Malformed =>
          malformed.logWithContext(
            Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
          )
          val hostedConfirmingPartiesF =
            topologySnapshot.canConfirm(
              sender,
              declaredConfirmingParties.map(_.party),
            )
          val res = hostedConfirmingPartiesF.map { hostedConfirmingParties =>
            loggingContext.debug(
              show"Malformed response $responseTimestamp for $viewKey considered as a rejection on behalf of $hostedConfirmingParties"
            )
            Some(hostedConfirmingParties): Option[Set[LfPartyId]]
          }
          OptionT(res)

        case _: LocalApprove | _: LocalReject =>
          val unexpectedConfirmingParties =
            confirmingParties -- declaredConfirmingParties.map(_.party)
          for {
            _ <-
              if (unexpectedConfirmingParties.isEmpty) OptionT.some[Future](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received a confirmation response at $responseTimestamp by $sender for request $requestId with unexpected confirming parties $unexpectedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[Future, Unit]
              }

            expectedConfirmingParties =
              declaredConfirmingParties.filter(p => confirmingParties.contains(p.party))
            unauthorizedConfirmingParties <- OptionT.liftF(
              topologySnapshot
                .canConfirm(
                  sender,
                  expectedConfirmingParties.map(_.party),
                )
                .map { confirmingParties =>
                  (expectedConfirmingParties.map(_.party) -- confirmingParties)
                }
            )
            _ <-
              if (unauthorizedConfirmingParties.isEmpty) OptionT.some[Future](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received an unauthorized confirmation response at $responseTimestamp by $sender for request $requestId on behalf of $unauthorizedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[Future, Unit]
              }
          } yield confirmingParties
      }

    for {
      _ <- OptionT.fromOption[Future](rootHashO.traverse_ { rootHash =>
        if (request.rootHash == rootHash) Some(())
        else {
          val cause =
            show"Received a confirmation response at $responseTimestamp by $sender for request $requestId with an invalid root hash $rootHash instead of ${request.rootHash}. Discarding response..."
          val alarm = MediatorError.MalformedMessage.Reject(cause)
          alarm.report()

          None
        }
      })

      viewKeysAndParties <- {
        viewKeyO match {
          case None =>
            // If no view key is given, the local verdict is Malformed and confirming parties is empty by the invariants of ConfirmationResponse.
            // We treat this as a rejection for all parties hosted by the participant.
            localVerdict match {
              case malformed: Malformed =>
                malformed.logWithContext(
                  Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
                )
              case other =>
                ErrorUtil.invalidState(s"Verdict should be of type malformed, but got $other")
            }

            val informeesByView = ViewKey[VKEY].informeesAndThresholdByKey(request)
            val ret = informeesByView.toList
              .parTraverseFilter { case (viewKey, (informees, _threshold)) =>
                val confirmingParties = informees.collect { case cp: ConfirmingParty => cp.party }
                topologySnapshot.canConfirm(sender, confirmingParties).map { partiesCanConfirm =>
                  val hostedConfirmingParties = confirmingParties.toSeq
                    .filter(partiesCanConfirm.contains(_))
                  Option.when(hostedConfirmingParties.nonEmpty)(
                    viewKey -> hostedConfirmingParties.toSet
                  )
                }
              }
              .map { viewsWithConfirmingPartiesForSender =>
                loggingContext.debug(
                  s"Malformed response $responseTimestamp from $sender considered as a rejection for ${viewsWithConfirmingPartiesForSender}"
                )
                viewsWithConfirmingPartiesForSender
              }
            OptionT.liftF(ret)
          case Some(viewKey) =>
            for {
              informeesAndThreshold <- OptionT.fromOption[Future](
                ViewKey[VKEY].informeesAndThresholdByKey(request).get(viewKey).orElse {
                  val cause =
                    s"Received a confirmation response at $responseTimestamp by $sender for request $requestId with an unknown view position $viewKey. Discarding response..."
                  val alarm = MediatorError.MalformedMessage.Reject(cause)
                  alarm.report()

                  None
                }
              )
              (informees, _) = informeesAndThreshold
              declaredConfirmingParties = informees.collect { case p: ConfirmingParty => p }
              authorizedConfirmingParties <- authorizedPartiesOfSender(
                viewKey,
                declaredConfirmingParties,
              )
            } yield List(viewKey -> authorizedConfirmingParties)
        }
      }
    } yield viewKeysAndParties
  }

}

trait ViewKey[VKEY] extends Pretty[VKEY] with Product with Serializable {
  def name: String

  def keyOfResponse(response: ConfirmationResponse): Option[VKEY]

  def informeesAndThresholdByKey(
      request: MediatorConfirmationRequest
  ): Map[VKEY, (Set[Informee], NonNegativeInt)]
}
object ViewKey {
  def apply[VKEY: ViewKey]: ViewKey[VKEY] = implicitly[ViewKey[VKEY]]

  implicit case object ViewPositionKey extends ViewKey[ViewPosition] {
    override def name: String = "view position"

    override def keyOfResponse(response: ConfirmationResponse): Option[ViewPosition] =
      response.viewPositionO

    override def informeesAndThresholdByKey(
        request: MediatorConfirmationRequest
    ): Map[ViewPosition, (Set[Informee], NonNegativeInt)] =
      request.informeesAndThresholdByViewPosition

    override def treeOf(t: ViewPosition): Tree = t.pretty.treeOf(t)
  }
}
