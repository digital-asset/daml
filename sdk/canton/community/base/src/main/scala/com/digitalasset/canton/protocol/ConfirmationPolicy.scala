// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.parallel.*
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
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LfTransactionUtil
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

sealed trait ConfirmationPolicy extends Product with Serializable with PrettyPrinting {
  protected val name: String
  protected val index: Int

  def toProtoPrimitive: ByteString = DeterministicEncoding.encodeString(name)

  /** Returns informees, participants hosting those informees,
    * their trust level and corresponding threshold for a given action node.
    */
  def informeesParticipantsAndThreshold(
      actionNode: LfActionNode,
      topologySnapshot: TopologySnapshot,
  )(implicit
      ec: ExecutionContext
  ): Future[(Map[LfPartyId, (Set[ParticipantId], NonNegativeInt, TrustLevel)], NonNegativeInt)]

  def informeesAndThreshold(actionNode: LfActionNode, topologySnapshot: TopologySnapshot)(implicit
      ec: ExecutionContext
  ): Future[(Set[Informee], NonNegativeInt)]

  /** The minimal acceptable trust level of the sender of mediator response */
  def requiredTrustLevel: TrustLevel

  /** The minimum threshold for views of requests with this policy.
    * The mediator checks that all views have at least the given threshold.
    */
  def minimumThreshold(confirmingParties: Set[ConfirmingParty]): NonNegativeInt = NonNegativeInt.one

  protected def additionalWeightOfSubmittingAdminParty(
      confirmingParties: Set[ConfirmingParty],
      adminParty: LfPartyId,
  ): NonNegativeInt =
    confirmingParties
      .collectFirst { case ConfirmingParty(`adminParty`, _, _) => NonNegativeInt.zero }
      .getOrElse(NonNegativeInt.one)

  /** Intended to be used for [[com.digitalasset.canton.version.ProtocolVersion.v6]] and higher.
    * This method adds an additional quorum with the submitting admin party with threshold 1, thus making sure
    * that the submitting admin party has to confirm the view for it to be accepted.
    */
  def withSubmittingAdminPartyQuorum(
      submittingAdminPartyO: Option[LfPartyId]
  )(viewConfirmationParameters: ViewConfirmationParameters): ViewConfirmationParameters =
    submittingAdminPartyO match {
      case Some(submittingAdminParty) =>
        val newQuorum = Quorum(
          Map(submittingAdminParty -> PositiveInt.one),
          NonNegativeInt.one,
        )

        val newQuorumList =
          if (viewConfirmationParameters.quorums.contains(newQuorum))
            viewConfirmationParameters.quorums
          else viewConfirmationParameters.quorums :+ newQuorum

        /* We are using tryCreate() because we are sure that the new confirmer is in the list of informees, since
         * it is added at the same time.
         */
        ViewConfirmationParameters.tryCreate(
          viewConfirmationParameters.informees
            + (submittingAdminParty -> viewConfirmationParameters.informees
              .getOrElse(submittingAdminParty, TrustLevel.Ordinary)),
          newQuorumList,
        )
      case None => viewConfirmationParameters
    }

  /** Intended to be used for [[com.digitalasset.canton.version.ProtocolVersion.v5]] and lower.
    * This method adds the submitting admin party to the quorum (if it is not already there) and updates the
    * threshold in a way that the view requires the acceptance of that party to be accepted.
    */
  def withSubmittingAdminParty(
      submittingAdminPartyO: Option[LfPartyId]
  )(viewConfirmationParameters: ViewConfirmationParameters): ViewConfirmationParameters =
    submittingAdminPartyO match {
      case Some(submittingAdminParty) =>
        // up until ProtocolVersion.v5 there is only one quorum
        val threshold = viewConfirmationParameters.quorums(0).threshold
        val confirmers = viewConfirmationParameters.confirmers
        val oldSubmittingInformee = confirmers
          .find(_.party == submittingAdminParty)
          .getOrElse(PlainInformee(submittingAdminParty))
        val additionalWeight =
          additionalWeightOfSubmittingAdminParty(
            confirmers,
            submittingAdminParty,
          )
        val newSubmittingInformee =
          oldSubmittingInformee.withAdditionalWeight(additionalWeight)

        val (newConfirmers, newThreshold) = newSubmittingInformee match {
          case newSubmittingConfirmingParty: ConfirmingParty =>
            oldSubmittingInformee match {
              case oldSubmittingConfirmingParty: ConfirmingParty =>
                (
                  confirmers - oldSubmittingConfirmingParty + newSubmittingConfirmingParty,
                  threshold + additionalWeight,
                )
              case _: PlainInformee =>
                (
                  confirmers + newSubmittingConfirmingParty,
                  threshold + additionalWeight,
                )
            }
          case _: PlainInformee => (confirmers, threshold)
        }
        /* we are using tryCreate() because we are sure that the new confirmer is in the list of informees, since
         * it is added at the same time
         */
        ViewConfirmationParameters.tryCreate(
          viewConfirmationParameters.informees +
            (newSubmittingInformee.party -> newSubmittingInformee.requiredTrustLevel),
          Seq(Quorum.create(newConfirmers, newThreshold)),
        )
      case _ => viewConfirmationParameters
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

    override def informeesParticipantsAndThreshold(
        node: LfActionNode,
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext
    ): Future[
      (Map[LfPartyId, (Set[ParticipantId], NonNegativeInt, TrustLevel)], NonNegativeInt)
    ] = {
      val stateVerifiers = LfTransactionUtil.stateKnownTo(node)
      topologySnapshot
        .activeParticipantsOfPartiesWithAttributes(node.informeesOfNode.toSeq)
        .map(informeesMap =>
          informeesMap.map { case (partyId, attributes) =>
            attributes.values.find(havingVip) match {
              // confirming party
              case Some(_) if stateVerifiers.contains(partyId) =>
                partyId -> (attributes.keySet, NonNegativeInt.one, TrustLevel.Vip)
              // plain informee
              case _ => partyId -> (attributes.keySet, NonNegativeInt.zero, TrustLevel.Ordinary)
            }
          }
        )
        // threshold is 1 because we only need one VIP party that confirms
        .map(informeesMap => (informeesMap, NonNegativeInt.one))
    }

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

    override def minimumThreshold(confirmingParties: Set[ConfirmingParty]): NonNegativeInt = {
      val weightOfOrdinary = confirmingParties.toSeq.collect {
        case ConfirmingParty(_, weight, TrustLevel.Ordinary) => weight.unwrap
      }.sum
      // Make sure that at least one VIP needs to approve.
      NonNegativeInt.tryCreate(weightOfOrdinary + 1)
    }

    override protected def additionalWeightOfSubmittingAdminParty(
        confirmingParties: Set[ConfirmingParty],
        adminParty: LfPartyId,
    ): NonNegativeInt = {
      // if the quorum only contains the admin submitting party then the additional weight is 0
      if (confirmingParties.size == 1 && confirmingParties.map(_.party).contains(adminParty))
        NonNegativeInt.zero
      else NonNegativeInt.tryCreate(confirmingParties.toSeq.map(_.weight.unwrap).sum + 1)
    }
  }

  case object Signatory extends ConfirmationPolicy {
    override val name = "Signatory"
    protected override val index: Int = 1

    private def getInformeesAndThreshold(node: LfActionNode): (Set[Informee], NonNegativeInt) = {
      val confirmingParties =
        LfTransactionUtil.signatoriesOrMaintainers(node) | LfTransactionUtil.actingParties(node)
      require(
        confirmingParties.nonEmpty,
        "There must be at least one confirming party, as every node must have at least one signatory.",
      )
      val plainInformees = node.informeesOfNode -- confirmingParties
      toInformeesAndThreshold(confirmingParties, plainInformees, TrustLevel.Ordinary)
    }

    override def informeesParticipantsAndThreshold(
        node: LfActionNode,
        topologySnapshot: TopologySnapshot,
    )(implicit
        ec: ExecutionContext
    ): Future[
      (Map[LfPartyId, (Set[ParticipantId], NonNegativeInt, TrustLevel)], NonNegativeInt)
    ] = {
      val (informees, threshold) = getInformeesAndThreshold(node)
      val confirmersIds = informees.collect { case cp: ConfirmingParty => cp.party }
      topologySnapshot
        .activeParticipantsOfPartiesWithAttributes(informees.map(_.party).toSeq)
        .map(informeesMap =>
          informeesMap.map { case (partyId, attributes) =>
            // confirming party
            if (confirmersIds.contains(partyId))
              partyId -> (attributes.keySet, NonNegativeInt.one, TrustLevel.Ordinary)
            // plain informee
            else partyId -> (attributes.keySet, NonNegativeInt.zero, TrustLevel.Ordinary)
          }
        )
        .map(informeesMap => (informeesMap, threshold))
    }

    override def informeesAndThreshold(node: LfActionNode, topologySnapshot: TopologySnapshot)(
        implicit ec: ExecutionContext
    ): Future[(Set[Informee], NonNegativeInt)] =
      Future.successful(
        getInformeesAndThreshold(node)
      )

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
