// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.Ior
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.topology.ExternalPartyOnboardingDetails.{
  ExternalPartyNamespace,
  SignedPartyToKeyMapping,
  SignedPartyToParticipant,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.PositiveSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyTransaction.PositiveTopologyTransaction
import com.digitalasset.canton.version.ProtocolVersion

import scala.reflect.ClassTag

/** Data class containing onbarding signed topology transactions for an external party.
  *
  * The following invariants are enforced in the create method of the companion object:
  *
  *   - The namespaces of all 3 transactions are consistent
  *   - There is at least one confirming participant
  *   - There is no participant with submission permission
  *
  * @param signedNamespaceTransaction
  *   Either a NamespaceDelegation or DecentralizedNamespaceDefinition
  * @param signedPartyToKeyMappingTransaction
  *   Party to Key mapping transaction
  * @param signedPartyToParticipantTransaction
  *   Party to Participant transaction
  * @param isConfirming
  *   True if the allocating node is a confirming node for the party
  */
final case class ExternalPartyOnboardingDetails private (
    signedNamespaceTransaction: ExternalPartyNamespace,
    signedPartyToKeyMappingTransaction: SignedPartyToKeyMapping,
    signedPartyToParticipantTransaction: SignedPartyToParticipant,
    isConfirming: Boolean,
) {

  /** Namespace of the external party. Either from a single or decentralized namespace
    */
  def namespace: Namespace = signedNamespaceTransaction.namespace

  /** PartyId of the external party
    */
  def partyId: PartyId = signedPartyToParticipantTransaction.mapping.partyId

  /** Party hint of the external party
    */
  def partyHint: String = partyId.uid.identifier.str

  /** Returns true if the party is multi hosted
    */
  def isMultiHosted: Boolean = hostingParticipants.sizeIs > 1

  def hostingParticipants: Seq[HostingParticipant] =
    signedPartyToParticipantTransaction.mapping.participants
  def confirmationThreshold: PositiveInt = signedPartyToParticipantTransaction.mapping.threshold
  def signingKeysThreshold: PositiveInt = signedPartyToKeyMappingTransaction.mapping.threshold
  def numberOfSigningKeys: Int = signedPartyToKeyMappingTransaction.mapping.signingKeys.length
}

object ExternalPartyOnboardingDetails {

  // Type aliases for conciseness
  private type SignedNamespaceDelegation =
    SignedTopologyTransaction[TopologyChangeOp.Replace, NamespaceDelegation]
  private type SignedDecentralizedNamespace =
    SignedTopologyTransaction[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
  private type SignedPartyToKeyMapping =
    SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping]
  private type SignedPartyToParticipant =
    SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]
  sealed trait ExternalPartyNamespace {
    def signedTransaction: PositiveSignedTopologyTransaction
    def namespace: Namespace = signedTransaction.mapping.namespace
  }
  final case class SingleNamespace(signedTransaction: SignedNamespaceDelegation)
      extends ExternalPartyNamespace
  final case class DecentralizedNamespace(signedTransaction: SignedDecentralizedNamespace)
      extends ExternalPartyNamespace

  // TODO(i27530): Should we check if it's a non fully authorized decentralized namespace definition?
  private def isProposal(
      transaction: PositiveTopologyTransaction,
      allocatingParticipantId: ParticipantId,
  ): Boolean = {
    // If the party is also hosted on other nodes, it needs to be a proposal,
    // as approval from the other nodes is needed to fully authorize the transaction
    val isHostedOnOtherNodes = transaction
      .selectMapping[PartyToParticipant]
      .toList
      .flatMap(_.mapping.participants.map(_.participantId))
      .exists(_ != allocatingParticipantId)

    isHostedOnOtherNodes
  }

  private def buildSignedTopologyTransactions(
      transactionsWithSignatures: NonEmpty[List[(PositiveTopologyTransaction, List[Signature])]],
      multiSignatures: List[Signature],
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
  ): Either[String, List[PositiveSignedTopologyTransaction]] = {
    val signedTransactionHashes = transactionsWithSignatures.map { case (transaction, _) =>
      transaction.hash
    }.toSet
    val multiTransactionSignatures = multiSignatures.map(
      MultiTransactionSignature(signedTransactionHashes, _)
    )
    transactionsWithSignatures.forgetNE.traverse { case (transaction, signatures) =>
      NonEmpty
        .from(
          multiTransactionSignatures ++ signatures.map(
            SingleTransactionSignature(transaction.hash, _)
          )
        )
        .toRight("Missing signatures")
        .flatMap(transactionSignatures =>
          SignedTopologyTransaction.create(
            transaction,
            transactionSignatures.toSet,
            isProposal = isProposal(transaction, participantId),
            protocolVersion,
          )
        )
    }
  }

  private def validateMaximumOneMapping[M <: TopologyMapping](
      transactions: List[PositiveSignedTopologyTransaction]
  )(implicit
      classTag: ClassTag[M]
  ): Either[String, Option[SignedTopologyTransaction[Replace, M]]] =
    transactions.flatMap(_.select[TopologyChangeOp.Replace, M]) match {
      case Nil => Right(None)
      case singleTransaction :: Nil => Right(Some(singleTransaction))
      case moreThanOneMapping =>
        Left(
          s"Only one transaction of type ${classTag.runtimeClass.getName} can be provided, got ${moreThanOneMapping.length}"
        )
    }
  private def validateExactlyOneMapping[M <: TopologyMapping](
      transactions: List[PositiveSignedTopologyTransaction]
  )(implicit
      classTag: ClassTag[M]
  ): Either[String, SignedTopologyTransaction[Replace, M]] = for {
    zeroOrOne <- validateMaximumOneMapping[M](transactions)
    exactlyOne <- zeroOrOne.toRight(
      s"At least one transaction of type ${classTag.runtimeClass.getName} must be provided, got 0"
    )
  } yield exactlyOne

  // Find either a NamespaceDelegation or a DecentralizedNamespace
  private def validateNamespace(
      signedTopologyTransactions: List[PositiveSignedTopologyTransaction]
  ): Either[String, ExternalPartyNamespace] = for {
    singleRootNamespaceO <- validateMaximumOneMapping[NamespaceDelegation](
      signedTopologyTransactions
    )
    _ <- Either.cond(
      singleRootNamespaceO.forall(tx =>
        tx.transaction.mapping.target.fingerprint == tx.transaction.mapping.namespace.fingerprint
      ),
      (),
      "Namespace delegation is not a root delegation. Ensure the target key fingerprint is the same as the namespace fingerprint",
    )
    _ <- Either.cond(
      singleRootNamespaceO.forall(tx => tx.transaction.mapping.restriction == CanSignAllMappings),
      (),
      "Namespace delegation must have a CanSignAllMappings restriction.",
    )
    // TODO(i27530): Do we need other validations for decentralized namespaces?
    decentralizedNamespaceO <- validateMaximumOneMapping[DecentralizedNamespaceDefinition](
      signedTopologyTransactions
    )
    namespace <- Ior
      .fromOptions(singleRootNamespaceO, decentralizedNamespaceO)
      .toRight("Either a NamespaceDelegation or a DecentralizedNamespace is required")
      .flatMap {
        _.bimap(
          SingleNamespace(_): ExternalPartyNamespace,
          DecentralizedNamespace(_): ExternalPartyNamespace,
        ).onlyLeftOrRight
          .toRight("Only one of NamespaceDelegation or DecentralizedNamespace can be provided")
          .map(_.merge)
      }
  } yield namespace

  private def validatePartyToParticipant(
      signedTopologyTransactions: List[PositiveSignedTopologyTransaction],
      participantId: ParticipantId,
      partyNamespace: Namespace,
  ): Either[String, (SignedPartyToParticipant, Boolean)] =
    for {
      signedPartyToParticipant <- validateExactlyOneMapping[PartyToParticipant](
        signedTopologyTransactions
      )
      hostingParticipants = signedPartyToParticipant.mapping.participants
      _ <- hostingParticipants.toList match {
        case HostingParticipant(hosting, permission, _onboarding) :: Nil =>
          Either.cond(
            hosting == participantId && permission == ParticipantPermission.Confirmation,
            (),
            s"The party is to be hosted on a single participant ($hosting) that is not this participant ($participantId). Submit the allocation request on $hosting instead.",
          )
        case _ => Right(())
      }
      nodePermissionsMap = signedPartyToParticipant.mapping.participants.groupMap(_.permission)(
        _.participantId
      )
      _ <- Either.cond(
        !nodePermissionsMap.contains(ParticipantPermission.Submission),
        (),
        "The PartyToParticipant transaction must not contain any node with Submission permission",
      )
      confirmingNodes = nodePermissionsMap.getOrElse(Confirmation, List.empty)
      _ <- Either.cond(
        confirmingNodes.nonEmpty,
        (),
        "The PartyToParticipant transaction must contain at least one node with Confirmation permission",
      )
      isConfirmingNode = confirmingNodes.contains(participantId)
      _ <- Either.cond(
        // If it's not a confirming node it should be an observing one, as external parties are not expected
        // to give Submission permission to any node
        Option
          .when(!isConfirmingNode)(
            nodePermissionsMap
              .getOrElse(ParticipantPermission.Observation, List.empty)
              .contains(participantId)
          )
          .forall(_ == true),
        (),
        s"This node is not hosting the party either with Confirmation or Observation permission.",
      )
      _ <- Either.cond(
        signedPartyToParticipant.mapping.namespace == partyNamespace,
        (),
        s"The PartyToParticipant namespace (${signedPartyToParticipant.mapping.namespace}) does not match the party's namespace ($partyNamespace)",
      )
    } yield (signedPartyToParticipant, isConfirmingNode)

  private def validatePartyToKey(
      signedTopologyTransactions: List[PositiveSignedTopologyTransaction],
      partyNamespace: Namespace,
  ): Either[String, SignedPartyToKeyMapping] = for {
    signedPartyToKey <- validateExactlyOneMapping[PartyToKeyMapping](signedTopologyTransactions)
    _ <- Either.cond(
      signedPartyToKey.mapping.namespace == partyNamespace,
      (),
      s"The PartyToKeyMapping namespace (${signedPartyToKey.mapping.namespace}) does not match the party's namespace ($partyNamespace)",
    )
  } yield signedPartyToKey

  def create(
      signedTransactions: NonEmpty[List[(PositiveTopologyTransaction, List[Signature])]],
      multiSignatures: List[Signature],
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
  ): Either[String, ExternalPartyOnboardingDetails] =
    for {
      signedTopologyTransactions <- buildSignedTopologyTransactions(
        signedTransactions,
        multiSignatures,
        protocolVersion,
        participantId,
      )
      namespaceTransaction <- validateNamespace(signedTopologyTransactions)
      namespace = namespaceTransaction.namespace
      partyToParticipantAndIsConfirming <- validatePartyToParticipant(
        signedTopologyTransactions,
        participantId,
        namespace,
      )
      (partyToParticipant, isConfirming) = partyToParticipantAndIsConfirming
      partyToKey <- validatePartyToKey(signedTopologyTransactions, namespace)
    } yield ExternalPartyOnboardingDetails(
      namespaceTransaction,
      partyToKey,
      partyToParticipant,
      isConfirming,
    )
}
