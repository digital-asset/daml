// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{ConfirmingParty, Informee, PlainInformee}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LfTransactionUtil
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

sealed trait ConfirmationPolicy extends Product with Serializable with PrettyPrinting {
  protected val name: String
  protected val index: Int

  def toProtoPrimitive: ByteString = DeterministicEncoding.encodeString(name)

  def informeesAndThreshold(actionNode: LfActionNode, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)]

  /** The minimal acceptable trust level of the sender of mediator response */
  def requiredTrustLevel: TrustLevel

  /** The minimum threshold for views of requests with this policy.
    * The mediator checks that all views have at least the given threshold.
    */
  def minimumThreshold(informees: Set[Informee]): NonNegativeInt = NonNegativeInt.one

  protected def additionalWeightOfSubmittingAdminParty(
      informees: Set[Informee],
      adminParty: LfPartyId,
  ): NonNegativeInt =
    informees
      .collectFirst { case ConfirmingParty(`adminParty`, _, _) => NonNegativeInt.zero }
      .getOrElse(NonNegativeInt.one)

  def withSubmittingAdminParty(
      submittingAdminPartyO: Option[LfPartyId]
  )(informees: Set[Informee], threshold: NonNegativeInt): (Set[Informee], NonNegativeInt) =
    submittingAdminPartyO match {
      case Some(submittingAdminParty) =>
        val oldSubmittingInformee = informees
          .find(_.party == submittingAdminParty)
          .getOrElse(PlainInformee(submittingAdminParty))
        val additionalWeight =
          additionalWeightOfSubmittingAdminParty(
            informees,
            submittingAdminParty,
          )
        val newSubmittingInformee =
          oldSubmittingInformee.withAdditionalWeight(additionalWeight)

        val newInformees = informees - oldSubmittingInformee + newSubmittingInformee
        val newThreshold = threshold + additionalWeight

        newInformees -> newThreshold

      case None => informees -> threshold
    }

  override def pretty: Pretty[ConfirmationPolicy] = prettyOfObject[ConfirmationPolicy]
}

object ConfirmationPolicy {

  private val havingVip: ParticipantAttributes => Boolean = _.trustLevel == TrustLevel.Vip
  private val havingConfirmer: ParticipantAttributes => Boolean = _.permission.canConfirm

  private def toInformeesAndThreshold(
      confirmingParties: Set[LfPartyId],
      plainInformees: Set[LfPartyId],
      requiredTrustLevel: TrustLevel,
  ): (Set[Informee], NonNegativeInt) = {
    // We make sure that the threshold is at least 1 so that a transaction is not vacuously approved if the confirming parties are empty.
    val threshold = NonNegativeInt.tryCreate(Math.max(confirmingParties.size, 1))
    val informees =
      confirmingParties.map(ConfirmingParty(_, PositiveInt.one, requiredTrustLevel): Informee) ++
        plainInformees.map(PlainInformee)
    (informees, threshold)
  }

  case object Vip extends ConfirmationPolicy {
    override val name = "Vip"
    protected override val index: Int = 0

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val stateVerifiers = LfTransactionUtil.stateKnownTo(node)
      val confirmingPartiesF = stateVerifiers.toList
        .parTraverseFilter { partyId =>
          topologySnapshot
            .activeParticipantsOf(partyId)
            .map(participants => participants.values.find(havingVip).map(_ => partyId))
        }
        .map(_.toSet)
      confirmingPartiesF.map { confirmingParties =>
        val plainInformees = node.informeesOfNode -- confirmingParties
        val informees =
          confirmingParties.map(ConfirmingParty(_, PositiveInt.one, TrustLevel.Vip)) ++
            plainInformees.map(PlainInformee)
        // As all VIP participants are trusted, it suffices that one of them confirms.
        (informees, NonNegativeInt.one)
      }
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Vip

    override def minimumThreshold(informees: Set[Informee]): NonNegativeInt = {
      // Make sure that at least one VIP needs to approve.

      val weightOfOrdinary = informees.toSeq.collect {
        case ConfirmingParty(_, weight, TrustLevel.Ordinary) => weight.unwrap
      }.sum
      NonNegativeInt.tryCreate(weightOfOrdinary + 1)
    }

    override protected def additionalWeightOfSubmittingAdminParty(
        informees: Set[Informee],
        adminParty: LfPartyId,
    ): NonNegativeInt =
      NonNegativeInt.tryCreate(informees.toSeq.map(_.weight.unwrap).sum + 1)
  }

  case object Signatory extends ConfirmationPolicy {
    override val name = "Signatory"
    protected override val index: Int = 1

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = node.informeesOfNode -- confirmingParties
      Future.successful(
        toInformeesAndThreshold(confirmingParties, plainInformees, TrustLevel.Ordinary)
      )
    }

    override def requiredTrustLevel: TrustLevel = TrustLevel.Ordinary
  }

  val values: Seq[ConfirmationPolicy] = Seq[ConfirmationPolicy](Vip, Signatory)

  require(
    values.zipWithIndex.forall { case (policy, index) => policy.index == index },
    "Mismatching policy indices.",
  )

  /** Ordering for [[ConfirmationPolicy]] */
  implicit val orderConfirmationPolicy: Order[ConfirmationPolicy] =
    Order.by[ConfirmationPolicy, Int](_.index)

  /** Chooses appropriate confirmation policies for a transaction.
    * It chooses [[Vip]] if every node has at least one VIP who knows the state
    * It chooses [[Signatory]] if every node has a Participant that can confirm.
    */
  def choose(transaction: LfVersionedTransaction, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext
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
    // TODO(i4930) - potentially batch this lookup
    val eligibleParticipantsF =
      allParties.toList
        .parTraverse(partyId =>
          topologySnapshot.activeParticipantsOf(partyId).map { res =>
            (partyId, (res.values.exists(havingVip), res.values.exists(havingConfirmer)))
          }
        )
        .map(_.toMap)

    eligibleParticipantsF.map { eligibleParticipants =>
      val hasVipForEachNode = vipCheckPartiesPerNode.forall {
        _.exists(eligibleParticipants(_)._1)
      }
      val hasConfirmersForEachNode = signatoriesCheckPartiesPerNode.forall { signatoriesForNode =>
        signatoriesForNode.nonEmpty && signatoriesForNode.forall(eligibleParticipants(_)._2)
      }
      List(hasVipForEachNode -> Vip, hasConfirmersForEachNode -> Signatory)
        .filter(_._1)
        .map(_._2)
    }
  }

  def fromProtoPrimitive(
      encodedName: ByteString
  ): Either[DeserializationError, ConfirmationPolicy] =
    DeterministicEncoding.decodeString(encodedName).flatMap {
      case (Vip.name, _) => Right(Vip)
      case (Signatory.name, _) => Right(Signatory)
      case (badName, _) =>
        Left(DefaultDeserializationError(s"Invalid confirmation policy $badName"))
    }
}
