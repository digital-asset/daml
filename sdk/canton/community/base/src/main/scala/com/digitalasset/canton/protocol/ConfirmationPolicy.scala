// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  ConfirmingParty,
  Informee,
  PlainInformee,
  Quorum,
  ViewConfirmationParameters,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LfTransactionUtil
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

sealed trait ConfirmationPolicy extends Product with Serializable with PrettyPrinting {
  protected val name: String
  protected val index: Int

  def toProtoPrimitive: ByteString = DeterministicEncoding.encodeString(name)

  /** Returns informees, participants hosting those informees,
    * and corresponding threshold for a given action node.
    */
  def informeesParticipantsAndThreshold(
      actionNode: LfActionNode,
      topologySnapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[(Map[LfPartyId, (Set[ParticipantId], NonNegativeInt)], NonNegativeInt)]

  def informeesAndThreshold(actionNode: LfActionNode, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[(Set[Informee], NonNegativeInt)]

  /** This method adds an additional quorum with the submitting admin party with threshold 1, thus making sure
    * that the submitting admin party has to confirm the view for it to be accepted.
    */
  def withSubmittingAdminParty(
      submittingAdminPartyO: Option[LfPartyId]
  )(viewConfirmationParameters: ViewConfirmationParameters): ViewConfirmationParameters =
    submittingAdminPartyO match {
      case Some(submittingAdminParty) =>
        val newQuorum = Quorum(
          Map(submittingAdminParty -> PositiveInt.one),
          NonNegativeInt.one,
        )

        if (viewConfirmationParameters.quorums.contains(newQuorum))
          viewConfirmationParameters
        else {
          val newQuorumList = viewConfirmationParameters.quorums :+ newQuorum
          /* We are using tryCreate() because we are sure that the new confirmer is in the list of informees, since
           * it is added at the same time.
           */
          ViewConfirmationParameters.tryCreate(
            viewConfirmationParameters.informees + submittingAdminParty,
            newQuorumList,
          )
        }
      case None => viewConfirmationParameters
    }

  override def pretty: Pretty[ConfirmationPolicy] = prettyOfObject[ConfirmationPolicy]
}

object ConfirmationPolicy {

  private val havingConfirmer: ParticipantAttributes => Boolean = _.permission.canConfirm

  private def toInformeesAndThreshold(
      confirmingParties: Set[LfPartyId],
      plainInformees: Set[LfPartyId],
  ): (Set[Informee], NonNegativeInt) = {
    // We make sure that the threshold is at least 1 so that a transaction is not vacuously approved if the confirming parties are empty.
    val threshold = NonNegativeInt.tryCreate(Math.max(confirmingParties.size, 1))
    val informees =
      confirmingParties.map(ConfirmingParty(_, PositiveInt.one): Informee) ++
        plainInformees.map(PlainInformee)
    (informees, threshold)
  }

  case object Signatory extends ConfirmationPolicy {
    override val name = "Signatory"
    protected override val index: Int = 0

    private def getInformeesAndThreshold(node: LfActionNode): (Set[Informee], NonNegativeInt) = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = node.informeesOfNode -- confirmingParties
      toInformeesAndThreshold(confirmingParties, plainInformees)
    }

    override def informeesParticipantsAndThreshold(
        node: LfActionNode,
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext,
        traceContext: TraceContext,
    ): Future[
      (Map[LfPartyId, (Set[ParticipantId], NonNegativeInt)], NonNegativeInt)
    ] = {
      val (informees, threshold) = getInformeesAndThreshold(node)
      val confirmersIds = informees.collect { case cp: ConfirmingParty => cp.party }
      topologySnapshot
        .activeParticipantsOfPartiesWithAttributes(informees.map(_.party).toSeq)
        .map(informeesMap =>
          informeesMap.map { case (partyId, attributes) =>
            // confirming party
            if (confirmersIds.contains(partyId))
              partyId -> (attributes.keySet, NonNegativeInt.one)
            // plain informee
            else partyId -> (attributes.keySet, NonNegativeInt.zero)
          }
        )
        .map(informeesMap => (informeesMap, threshold))
    }

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit
        ec: ExecutionContext,
        tc: TraceContext,
    ): Future[(Set[Informee], NonNegativeInt)] =
      Future.successful(
        getInformeesAndThreshold(node)
      )
  }

  val values: Seq[ConfirmationPolicy] = Seq[ConfirmationPolicy](Signatory)

  require(
    values.zipWithIndex.forall { case (policy, index) => policy.index == index },
    "Mismatching policy indices.",
  )

  /** Ordering for [[ConfirmationPolicy]] */
  implicit val orderConfirmationPolicy: Order[ConfirmationPolicy] =
    Order.by[ConfirmationPolicy, Int](_.index)

  /** Chooses appropriate confirmation policies for a transaction.
    * It chooses [[Signatory]] if every node has a Participant that can confirm.
    */
  def choose(transaction: LfVersionedTransaction, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Seq[ConfirmationPolicy]] = {

    val actionNodes = transaction.nodes.values.collect { case an: LfActionNode => an }

    val vipCheckPartiesPerNode = actionNodes.map { node =>
      node.informeesOfNode & LfTransactionUtil.stateKnownTo(node)
    }
    val signatoriesCheckPartiesPerNode = actionNodes.map { node =>
      LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
    }
    val allParties =
      (vipCheckPartiesPerNode.flatten ++ signatoriesCheckPartiesPerNode.flatten).toSet
    val eligibleParticipantsF =
      topologySnapshot.activeParticipantsOfPartiesWithAttributes(allParties.toSeq).map { result =>
        result.map { case (party, attributesMap) =>
          (party, attributesMap.values.exists(havingConfirmer))
        }
      }

    eligibleParticipantsF.map { eligibleParticipants =>
      val hasConfirmersForEachNode = signatoriesCheckPartiesPerNode.forall { signatoriesForNode =>
        signatoriesForNode.nonEmpty && signatoriesForNode.forall(eligibleParticipants(_))
      }
      List(hasConfirmersForEachNode -> Signatory)
        .filter(_._1)
        .map(_._2)
    }
  }

  def fromProtoPrimitive(
      encodedName: ByteString
  ): Either[DeserializationError, ConfirmationPolicy] =
    DeterministicEncoding.decodeString(encodedName).flatMap {
      case (Signatory.name, _) => Right(Signatory)
      case (badName, _) =>
        Left(DefaultDeserializationError(s"Invalid confirmation policy $badName"))
    }
}
