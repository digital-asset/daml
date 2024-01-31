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
import com.digitalasset.canton.protocol.messages.{
  LocalApprove,
  LocalReject,
  LocalVerdict,
  Malformed,
  MediatorRequest,
  MediatorResponse,
}
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash}
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

  def request: MediatorRequest

  /** The sequencer timestamp of the most recent message that affected this [[ResponseAggregator]]
    */
  def version: CantonTimestamp

  def isFinalized: Boolean

  /** Validate the additional confirmation response received and record if unless already finalized.
    */
  def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
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
            declaredConfirmingParties.toList
              .parFilterA(p => topologySnapshot.canConfirm(sender, p.party, p.requiredTrustLevel))
              .map(_.toSet)
          val res = hostedConfirmingPartiesF.map { hostedConfirmingParties =>
            loggingContext.debug(
              show"Malformed response $responseTimestamp for $viewKey considered as a rejection on behalf of $hostedConfirmingParties"
            )
            Some(hostedConfirmingParties.map(_.party)): Option[Set[LfPartyId]]
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
                    s"Received a mediator response at $responseTimestamp by $sender for request $requestId with unexpected confirming parties $unexpectedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[Future, Unit]
              }

            expectedConfirmingParties =
              declaredConfirmingParties.filter(p => confirmingParties.contains(p.party))
            unauthorizedConfirmingParties <- OptionT.liftF(
              expectedConfirmingParties.toList
                .parFilterA { p =>
                  topologySnapshot.canConfirm(sender, p.party, p.requiredTrustLevel).map(x => !x)
                }
                .map(_.map(_.party))
                .map(_.toSet)
            )
            _ <-
              if (unauthorizedConfirmingParties.isEmpty) OptionT.some[Future](())
              else {
                MediatorError.MalformedMessage
                  .Reject(
                    s"Received an unauthorized mediator response at $responseTimestamp by $sender for request $requestId on behalf of $unauthorizedConfirmingParties. Discarding response..."
                  )
                  .report()
                OptionT.none[Future, Unit]
              }
          } yield confirmingParties
      }

    for {
      _ <- OptionT.fromOption[Future](rootHashO.traverse_ { rootHash =>
        if (request.rootHash.forall(_ == rootHash)) Some(())
        else {
          val cause =
            show"Received a mediator response at $responseTimestamp by $sender for request $requestId with an invalid root hash $rootHash instead of ${request.rootHash.showValueOrNone}. Discarding response..."
          val alarm = MediatorError.MalformedMessage.Reject(cause)
          alarm.report()

          None
        }
      })

      viewKeysAndParties <- {
        viewKeyO match {
          case None =>
            // If no view key is given, the local verdict is Malformed and confirming parties is empty by the invariants of MediatorResponse.
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
                val hostedConfirmingPartiesF = informees.toList.parTraverseFilter {
                  case ConfirmingParty(party, _, requiredTrustLevel) =>
                    topologySnapshot
                      .canConfirm(sender, party, requiredTrustLevel)
                      .map(x => if (x) Some(party) else None)
                  case _ => Future.successful(None)
                }
                hostedConfirmingPartiesF.map { hostedConfirmingParties =>
                  if (hostedConfirmingParties.nonEmpty)
                    Some(viewKey -> hostedConfirmingParties.toSet)
                  else None
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
                    s"Received a mediator response at $responseTimestamp by $sender for request $requestId with an unknown view position $viewKey. Discarding response..."
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

  def keyOfResponse(response: MediatorResponse): Option[VKEY]

  def informeesAndThresholdByKey(
      request: MediatorRequest
  ): Map[VKEY, (Set[Informee], NonNegativeInt)]
}
object ViewKey {
  def apply[VKEY: ViewKey]: ViewKey[VKEY] = implicitly[ViewKey[VKEY]]

  implicit case object ViewPositionKey extends ViewKey[ViewPosition] {
    override def name: String = "view position"

    override def keyOfResponse(response: MediatorResponse): Option[ViewPosition] =
      response.viewPositionO

    override def informeesAndThresholdByKey(
        request: MediatorRequest
    ): Map[ViewPosition, (Set[Informee], NonNegativeInt)] =
      request.informeesAndThresholdByViewPosition

    override def treeOf(t: ViewPosition): Tree = t.pretty.treeOf(t)
  }
  implicit case object ViewHashKey extends ViewKey[ViewHash] {
    override def name: String = "view hash"

    override def keyOfResponse(response: MediatorResponse): Option[ViewHash] = None

    override def informeesAndThresholdByKey(
        request: MediatorRequest
    ): Map[ViewHash, (Set[Informee], NonNegativeInt)] =
      request.informeesAndThresholdByViewHash

    override def treeOf(t: ViewHash): Tree = t.pretty.treeOf(t)
  }
}
