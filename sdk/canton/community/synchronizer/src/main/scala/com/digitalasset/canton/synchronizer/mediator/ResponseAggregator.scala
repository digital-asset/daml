// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.OptionT
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ViewConfirmationParameters, ViewPosition}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import pprint.Tree

import scala.concurrent.ExecutionContext

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

  /** Validate the additional confirmation response received and record unless already finalized.
    */
  def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: ConfirmationResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[ResponseAggregation[VKey]]]

  protected def validateResponse[VKEY: ViewKey](
      viewKeyO: Option[VKEY],
      rootHash: RootHash,
      responseTimestamp: CantonTimestamp,
      sender: ParticipantId,
      localVerdict: LocalVerdict,
      topologySnapshot: TopologySnapshot,
      confirmingParties: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      loggingContext: NamedLoggingContext,
  ): OptionT[FutureUnlessShutdown, List[(VKEY, Set[LfPartyId])]] = {
    implicit val tc: TraceContext = loggingContext.traceContext
    def authorizedPartiesOfSender(
        viewKey: VKEY,
        declaredConfirmingParties: Set[LfPartyId],
    ): OptionT[FutureUnlessShutdown, Set[LfPartyId]] =
      localVerdict match {
        case malformed: LocalReject if malformed.isMalformed =>
          malformed.logWithContext(
            Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
          )
          val hostedConfirmingPartiesF =
            topologySnapshot.canConfirm(
              sender,
              declaredConfirmingParties,
            )
          val res = hostedConfirmingPartiesF.map { hostedConfirmingParties =>
            loggingContext.debug(
              show"Malformed response $responseTimestamp for $viewKey considered as a rejection on behalf of $hostedConfirmingParties"
            )
            Some(hostedConfirmingParties): Option[Set[LfPartyId]]
          }
          OptionT(res)
        case _: LocalVerdict =>
          val unexpectedConfirmingParties =
            confirmingParties -- declaredConfirmingParties
          for {
            _ <-
              if (unexpectedConfirmingParties.isEmpty) OptionT.some[FutureUnlessShutdown](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received a confirmation response at $responseTimestamp by $sender for request $requestId with unexpected confirming parties $unexpectedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[FutureUnlessShutdown, Unit]
              }

            expectedConfirmingParties =
              declaredConfirmingParties.intersect(confirmingParties)
            unauthorizedConfirmingParties <- OptionT.liftF(
              topologySnapshot
                .canConfirm(
                  sender,
                  expectedConfirmingParties,
                )
                .map { confirmingParties =>
                  expectedConfirmingParties -- confirmingParties
                }
            )
            _ <-
              if (unauthorizedConfirmingParties.isEmpty) OptionT.some[FutureUnlessShutdown](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received an unauthorized confirmation response at $responseTimestamp by $sender for request $requestId on behalf of $unauthorizedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[FutureUnlessShutdown, Unit]
              }
          } yield confirmingParties
      }

    for {
      _ <- OptionT.fromOption[FutureUnlessShutdown](
        if (request.rootHash == rootHash) Some(())
        else {
          val cause =
            show"Received a confirmation response at $responseTimestamp by $sender for request $requestId with an invalid root hash $rootHash instead of ${request.rootHash}. Discarding response..."
          val alarm = MediatorError.MalformedMessage.Reject(cause)
          alarm.report()

          None
        }
      )

      viewKeysAndParties <- {
        viewKeyO match {
          case None =>
            // If no view key is given, the local verdict is Malformed and confirming parties is empty by the invariants of ConfirmationResponse.
            // We treat this as a rejection for all parties hosted by the participant.
            localVerdict match {
              case malformed @ LocalReject(_, true) =>
                malformed.logWithContext(
                  Map("requestId" -> requestId.toString, "reportedBy" -> show"$sender")
                )
              case other =>
                ErrorUtil.invalidState(s"Verdict should be of type malformed, but got $other")
            }

            val informeesByView = ViewKey[VKEY].informeesAndThresholdByKey(request)
            val ret = informeesByView.toList
              .parTraverseFilter { case (viewKey, viewConfirmationParameters) =>
                val confirmingParties = viewConfirmationParameters.confirmers
                topologySnapshot.canConfirm(sender, confirmingParties).map { partiesCanConfirm =>
                  val hostedConfirmingParties = confirmingParties.toSeq
                    .filter(partiesCanConfirm.contains)
                  Option.when(hostedConfirmingParties.nonEmpty)(
                    viewKey -> hostedConfirmingParties.toSet
                  )
                }
              }
              .map { viewsWithConfirmingPartiesForSender =>
                loggingContext.debug(
                  s"Malformed response $responseTimestamp from $sender considered as a rejection for $viewsWithConfirmingPartiesForSender"
                )
                viewsWithConfirmingPartiesForSender
              }
            OptionT.liftF(ret)
          case Some(viewKey) =>
            for {
              viewConfirmationParameters <- OptionT.fromOption[FutureUnlessShutdown](
                ViewKey[VKEY].informeesAndThresholdByKey(request).get(viewKey).orElse {
                  val cause =
                    s"Received a confirmation response at $responseTimestamp by $sender for request $requestId with an unknown view position $viewKey. Discarding response..."
                  val alarm = MediatorError.MalformedMessage.Reject(cause)
                  alarm.report()

                  None
                }
              )
              declaredConfirmingParties = viewConfirmationParameters.confirmers
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
  ): Map[VKEY, ViewConfirmationParameters]
}
object ViewKey {
  def apply[VKEY: ViewKey]: ViewKey[VKEY] = implicitly[ViewKey[VKEY]]

  implicit case object ViewPositionKey extends ViewKey[ViewPosition] {
    override def name: String = "view position"

    override def keyOfResponse(response: ConfirmationResponse): Option[ViewPosition] =
      response.viewPositionO

    override def informeesAndThresholdByKey(
        request: MediatorConfirmationRequest
    ): Map[ViewPosition, ViewConfirmationParameters] =
      request.informeesAndConfirmationParamsByViewPosition

    override def treeOf(t: ViewPosition): Tree = t.pretty.treeOf(t)
  }
}
