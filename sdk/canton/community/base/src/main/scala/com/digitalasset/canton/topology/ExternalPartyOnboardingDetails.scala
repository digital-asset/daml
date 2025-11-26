// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.Ior
import cats.syntax.alternative.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Signature, SigningKeyUsage}
import com.digitalasset.canton.topology.ExternalPartyOnboardingDetails.{
  Centralized,
  Decentralized,
  OptionallySignedPartyToParticipant,
  PartyNamespace,
  SignedPartyToKeyMapping,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.{
  GenericSignedTopologyTransaction,
  PositiveSignedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyTransaction.PositiveTopologyTransaction
import com.digitalasset.canton.version.ProtocolVersion

import scala.reflect.ClassTag

/** Data class containing onboarding signed topology transactions for an external party. The
  * constructor of this class ensures that only transactions related to the creation of an external
  * party can be submitted. It performs validations on the kind of transactions and their
  * relationship with each other. It does NOT validate anything related to the authorization of
  * those transactions. That logic is implemented in the topology manager.
  *
  * @param partyNamespace
  *   Fully authorized party namespace. Can be either a single namespace or decentralized one with
  *   accompanying individual namespace owner transactions
  * @param signedPartyToKeyMappingTransaction
  *   Party to Key mapping transaction
  * @param optionallySignedPartyToParticipant
  *   Party to Participant transaction, either signed or unsigned
  * @param isConfirming
  *   True if the allocating node is a confirming node for the party
  */
final case class ExternalPartyOnboardingDetails private (
    partyNamespace: Option[PartyNamespace],
    signedPartyToKeyMappingTransaction: Option[SignedPartyToKeyMapping],
    optionallySignedPartyToParticipant: OptionallySignedPartyToParticipant,
    isConfirming: Boolean,
) {
  // Invariants
  require(
    !optionallySignedPartyToParticipant.mapping.participants
      .map(_.permission)
      .contains(ParticipantPermission.Submission),
    "External party cannot be hosted with Submission permission",
  )
  require(
    partyNamespace.forall(_.namespace == optionallySignedPartyToParticipant.mapping.namespace),
    "The party namespace does not match the PartyToParticipant namespace",
  )
  require(
    partyNamespace.forall {
      case decentralized: Decentralized =>
        decentralized.individualNamespaceTransaction.sizeIs <= ExternalPartyOnboardingDetails.maxDecentralizedOwnersSize.value
      case _ => true
    },
    "Decentralized namespace has over the maximum limit of namespace owners",
  )
  require(
    signedPartyToKeyMappingTransaction.forall(
      _.mapping.namespace == optionallySignedPartyToParticipant.mapping.namespace
    ),
    "The PartyToKeyMapping namespace does not match the PartyToParticipant namespace",
  )

  /** Return true if we expect the party to be fully allocated and authorized with the provided
    * transactions
    */
  def fullyAllocatesParty: Boolean = {
    val isCentralizedNamespace = partyNamespace.exists {
      case _: Centralized => true
      case _ => false
    }
    // Of course we don't know here if the signature is valid, but that will be checked
    // in the topology manager. Here we just see if there's a signature on the PTP supposed to cover the namespace
    val isSelfSignedPTP = optionallySignedPartyToParticipant match {
      case ExternalPartyOnboardingDetails.SignedPartyToParticipant(signed) =>
        signed.signatures.map(_.authorizingLongTermKey).contains(namespace.fingerprint)
      case ExternalPartyOnboardingDetails.UnsignedPartyToParticipant(unsigned) =>
        false
    }
    // Expect fully allocated if there's a centralized namespace, as we expect a signed root NSD
    // OR if it's a self-signed PTP
    // AND is not multi hosted
    (isCentralizedNamespace || isSelfSignedPTP) && hostingParticipants.sizeIs == 1

    // Note: It could be fully allocated as well with a decentralized namespace but checking this
    // would require re-running the authorization checks implemented in the topology manager
  }

  /** Namespace of the external party.
    */
  def namespace: Namespace = optionallySignedPartyToParticipant.mapping.namespace

  /** PartyId of the external party
    */
  def partyId: PartyId = optionallySignedPartyToParticipant.mapping.partyId

  /** Party hint of the external party
    */
  def partyHint: String = partyId.uid.identifier.str

  def hostingParticipants: Seq[HostingParticipant] =
    optionallySignedPartyToParticipant.mapping.participants
  def confirmationThreshold: PositiveInt = optionallySignedPartyToParticipant.mapping.threshold
}

object ExternalPartyOnboardingDetails {

  // Maximum number of decentralized namespace owners allowed through the `allocateExternalParty` API
  // This is hardcoded here to avoid unreasonably high number of namespace owner transactions to be distributed
  // through this endpoint, as the DecentralizedNamespaceDefinition itself does not have any limit on the number of
  // namespace owners. If this limit is too low for a given use case, go through the Admin API topology write service instead.
  // TODO(i27530): Make this configurable, or lift it when DecentralizedNamespaceDefinition has a limit
  val maxDecentralizedOwnersSize: PositiveInt = PositiveInt.tryCreate(10)

  // Type aliases for conciseness
  private type SignedNamespaceDelegation =
    SignedTopologyTransaction[TopologyChangeOp.Replace, NamespaceDelegation]
  private type SignedDecentralizedNamespace =
    SignedTopologyTransaction[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
  private type SignedPartyToKeyMapping =
    SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping]

  /** External party namespace, can be either centralied or decentralized
    */
  sealed trait PartyNamespace {

    /** Transactions to be loaded in the topology manager to create the party's namespace
      */
    def signedTransactions: Seq[GenericSignedTopologyTransaction]

    /** Namespace of the party
      */
    def namespace: Namespace
  }

  /** Decentralized party namespace. All transactions are expected to have all required signatures
    * to be fully authorized. If not the party allocation will fail in the topology manager auth
    * checks.
    * @param decentralizedTransaction
    *   The decentralized namespace transaction
    * @param individualNamespaceTransaction
    *   The individual namespace owner transactions
    */
  final private case class Decentralized(
      decentralizedTransaction: SignedDecentralizedNamespace,
      individualNamespaceTransaction: Seq[SignedNamespaceDelegation],
  ) extends PartyNamespace {
    // In that order on purpose, as the individual namespaces must be processed before the decentralized namespace can be authorized
    override def signedTransactions: Seq[GenericSignedTopologyTransaction] =
      individualNamespaceTransaction :+ decentralizedTransaction
    override def namespace: Namespace = decentralizedTransaction.mapping.namespace
  }

  /** Centralized party namespace. The transaction is expected to be fully authorized/ If not the
    * party allocation will fail in the topology manager auth checks.
    * @param singleTransaction
    *   The signed namespace definition transaction
    */
  final private case class Centralized(singleTransaction: SignedNamespaceDelegation)
      extends PartyNamespace {
    override def signedTransactions: Seq[GenericSignedTopologyTransaction] = Seq(singleTransaction)
    override def namespace: Namespace = singleTransaction.mapping.namespace
  }

  /** The PartyToParticipant mapping may be submitted signed (by the party's namespace) or unsigned
    * (by hosting nodes wanting to authorize the hosting). This trait makes the distinction between
    * the two cases.
    */
  sealed trait OptionallySignedPartyToParticipant {
    def mapping: PartyToParticipant
  }
  final case class SignedPartyToParticipant(
      signed: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]
  ) extends OptionallySignedPartyToParticipant {
    def mapping: PartyToParticipant = signed.mapping
  }
  final case class UnsignedPartyToParticipant(
      unsigned: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]
  ) extends OptionallySignedPartyToParticipant {
    def mapping: PartyToParticipant = unsigned.mapping
  }

  private val expectedTransactionMappings = Seq(
    NamespaceDelegation,
    DecentralizedNamespaceDefinition,
    PartyToParticipant,
    PartyToKeyMapping,
  )

  // TODO(i27530): We may be able to be more precise for P2P and P2K but it's hard to tell
  // at this stage especially with decentralized namespaces
  private def isProposal(
      transaction: PositiveTopologyTransaction
  ): Boolean =
    // Namespaces must be fully authorized so they can't be proposals
    transaction.selectMapping[NamespaceDelegation].isEmpty &&
      transaction.selectMapping[DecentralizedNamespaceDefinition].isEmpty

  /** Build SignedTopologyTransactions for transactions that have at least one signature.
    * Transactions without signatures are returned separately.
    */
  private def parseTransactionsWithSignatures(
      transactionsWithSignatures: NonEmpty[List[(PositiveTopologyTransaction, List[Signature])]],
      multiSignatures: List[Signature],
      protocolVersion: ProtocolVersion,
  ): Either[
    String,
    (List[PositiveSignedTopologyTransaction], List[PositiveTopologyTransaction]),
  ] = {
    val transactionHashes = transactionsWithSignatures.map { case (transaction, _) =>
      transaction.hash
    }.toSet
    val multiTransactionSignatures =
      multiSignatures.map(MultiTransactionSignature(transactionHashes, _))

    // Gather the signatures for a transaction. If no signatures can be found return None
    def signaturesForTransaction(
        transaction: PositiveTopologyTransaction,
        singleTransactionSignatures: Seq[Signature],
        multiTransactionSignatures: Seq[MultiTransactionSignature],
    ): Option[NonEmpty[Seq[TopologyTransactionSignature]]] = {
      // Deduplicate signatures to keep only one per signer
      val deduplicatedSignatures = NonEmpty
        .from(
          (singleTransactionSignatures.map(
            SingleTransactionSignature(transaction.hash, _)
          ) ++ multiTransactionSignatures)
            // Prefer single transaction signatures over multi transaction ones
            // as they're smaller (they don't need the list of signed hashes)
            .groupMapReduce(_.authorizingLongTermKey)(identity)((first, _) => first)
            .values
            .toSeq
        )
      transaction.mapping match {
        // Special case for root namespaces: they require signatures only from the namespace key
        case namespaceDelegation: NamespaceDelegation =>
          deduplicatedSignatures
            .map(_.filter(_.authorizingLongTermKey == namespaceDelegation.namespace.fingerprint))
            .flatMap(NonEmpty.from)
        case _ => deduplicatedSignatures
      }
    }

    transactionsWithSignatures.forgetNE
      .traverse { case (transaction, signatures) =>
        signaturesForTransaction(transaction, signatures, multiTransactionSignatures)
          .traverse(transactionSignatures =>
            SignedTopologyTransaction.create(
              transaction,
              transactionSignatures.toSet,
              isProposal = isProposal(transaction),
              protocolVersion,
            )
          )
          .map(_.map(Left(_)).getOrElse(Right(transaction)))
      }
      .map(_.separate)
  }

  private def validateMaximumOneElement[T](
      list: List[T],
      error: Int => String,
  ): Either[String, Option[T]] = list match {
    case Nil => Right(None)
    case singleTransaction :: Nil => Right(Some(singleTransaction))
    case moreThanOneMapping => Left(error(moreThanOneMapping.length))
  }

  private def validateMaximumOneMapping[M <: TopologyMapping](
      transactions: List[PositiveSignedTopologyTransaction]
  )(implicit
      classTag: ClassTag[M]
  ): Either[String, Option[SignedTopologyTransaction[Replace, M]]] =
    validateMaximumOneElement(
      transactions.flatMap(_.select[TopologyChangeOp.Replace, M]),
      length =>
        s"Only one transaction of type ${classTag.runtimeClass.getName} can be provided, got $length",
    )

  /*
   * Look for either a Decentralized namespace with optionally its individual namespace delegation, or a single namespace
   * Optional because one may only provide a PartyToParticipant transaction to authorize the hosting.
   */
  private def validatePartyNamespace(
      signedTransactions: List[PositiveSignedTopologyTransaction],
      p2pNamespace: Namespace,
  ): Either[String, Option[PartyNamespace]] =
    for {
      // Look first for a decentralized namespace, can only be at most one
      signedDecentralizedTxO <- validateMaximumOneMapping[DecentralizedNamespaceDefinition](
        signedTransactions
      )
      partyNamespaceO <- signedDecentralizedTxO match {
        case Some(signedDecentralizedTx) =>
          // If there's one, get the corresponding NamespaceDelegations for it
          val namespaceOwners = signedTransactions
            .flatMap(_.select[Replace, NamespaceDelegation])
            .filter(namespaceTx =>
              signedDecentralizedTx.mapping.owners.contains(namespaceTx.mapping.namespace)
            )
          Either.cond(
            namespaceOwners.sizeIs <= maxDecentralizedOwnersSize.value,
            Decentralized(
              signedDecentralizedTx,
              namespaceOwners,
            ).some,
            "Decentralized namespaces cannot have more than " +
              s"${maxDecentralizedOwnersSize.value} individual namespace owners, got ${namespaceOwners.size}",
          )
        case None =>
          // Otherwise look for a single delegation
          for {
            namespaceDelegationO <- validateMaximumOneMapping[NamespaceDelegation](
              signedTransactions
            )
            _ <- Either.cond(
              namespaceDelegationO.forall(NamespaceDelegation.isRootCertificate),
              (),
              "NamespaceDelegation is not a root namespace. Ensure the namespace and target key are the same",
            )
          } yield namespaceDelegationO.map(Centralized(_): PartyNamespace)
      }
      _ <- partyNamespaceO.traverse(partyNamespace =>
        Either.cond(
          partyNamespace.namespace == p2pNamespace,
          (),
          s"The Party namespace (${partyNamespace.namespace}) does not match the PartyToParticipant namespace ($p2pNamespace)",
        )
      )
    } yield partyNamespaceO

  private def validateExactlyOnePartyToParticipant(
      signedTransactions: List[PositiveSignedTopologyTransaction],
      unsignedTransactions: List[PositiveTopologyTransaction],
  ): Either[String, OptionallySignedPartyToParticipant] =
    // Check first if there's a signed P2P
    validateMaximumOneMapping[PartyToParticipant](signedTransactions)
      .flatMap {
        case Some(signed) => Right(SignedPartyToParticipant(signed))
        case None =>
          // Otherwise there must be an unsigned one
          validateMaximumOneElement(
            unsignedTransactions.flatMap(_.select[TopologyChangeOp.Replace, PartyToParticipant]),
            length =>
              s"Only one transaction of type PartyToParticipant can be provided, got $length",
          ).flatMap(
            _.toRight(s"One transaction of type PartyToParticipant must be provided, got 0")
          ).map(UnsignedPartyToParticipant(_))
      }

  /** Find and validate the PartyToParticipant transaction. It can be either signed (by the party
    * namespace) Or unsigned, in which case it will be signed by this participant (if it can) to
    * authorize the hosting of the party
    */
  private def validatePartyToParticipant(
      signedTopologyTransactions: List[PositiveSignedTopologyTransaction],
      unsignedTopologyTransactions: List[PositiveTopologyTransaction],
      participantId: ParticipantId,
  ): Either[String, (OptionallySignedPartyToParticipant, Boolean)] =
    for {
      optionallySignedPartyToParticipant <- validateExactlyOnePartyToParticipant(
        signedTopologyTransactions,
        unsignedTopologyTransactions,
      )
      signingKeys = optionallySignedPartyToParticipant.mapping.partySigningKeys
      hostingParticipants = optionallySignedPartyToParticipant.mapping.participants
      nodePermissionsMap = optionallySignedPartyToParticipant.mapping.participants
        .groupMap(_.permission)(_.participantId)
      nodesWithSubmissionPermission = nodePermissionsMap.getOrElse(
        ParticipantPermission.Submission,
        Seq.empty,
      )
      _ <- Either.cond(
        nodesWithSubmissionPermission.isEmpty,
        (),
        s"The PartyToParticipant transaction must not contain any node with Submission permission. Nodes with submission permission: ${nodesWithSubmissionPermission
            .mkString(", ")}",
      )
      _ <- hostingParticipants.toList match {
        case HostingParticipant(hosting, permission, _onboarding) :: Nil =>
          Either.cond(
            hosting == participantId && permission == ParticipantPermission.Confirmation,
            (),
            s"The party is to be hosted on a single participant ($hosting) that is not this participant ($participantId). Submit the allocation request on $hosting instead.",
          )
        case _ => Right(())
      }
      confirmingNodes = nodePermissionsMap.getOrElse(ParticipantPermission.Confirmation, List.empty)
      _ <- Either.cond(
        confirmingNodes.nonEmpty,
        (),
        "The PartyToParticipant transaction must contain at least one node with Confirmation permission",
      )
      _ <- Either.cond(
        signingKeys.forall(_.usage.contains(SigningKeyUsage.Protocol)),
        (),
        "Missing Protocol usage on signing keys",
      )
      _ <- Either.cond(
        signingKeys
          .find(_.fingerprint == optionallySignedPartyToParticipant.mapping.partyId.fingerprint)
          .forall(key =>
            key.usage.contains(SigningKeyUsage.Namespace) && key.usage
              .contains(SigningKeyUsage.Protocol)
          ),
        (),
        "Missing Namespace and Protocol usage on the party namespace key",
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
    } yield (optionallySignedPartyToParticipant, isConfirmingNode)

  /** Find at most one PartyToKeyMapping, for backwards compatibility of the endpoint. New parties
    * should be created with their keys in the PartyToParticipant
    */
  private def validatePartyToKey(
      signedTopologyTransactions: List[PositiveSignedTopologyTransaction],
      ptpNamespace: Namespace,
  ): Either[String, Option[SignedPartyToKeyMapping]] = for {
    signedPartyToKeyO <- validateMaximumOneMapping[PartyToKeyMapping](signedTopologyTransactions)
    _ <- signedPartyToKeyO.traverse(signedPartyToKey =>
      Either.cond(
        signedPartyToKey.mapping.namespace == ptpNamespace,
        (),
        s"The PartyToKeyMapping namespace (${signedPartyToKey.mapping.namespace}) does not match the PartyToParticipant namespace ($ptpNamespace)",
      )
    )
  } yield signedPartyToKeyO

  private def validateExactlyOneSetOfSigningKeys(
      partyToParticipant: PartyToParticipant,
      partyToKeyMappingO: Option[PartyToKeyMapping],
  ) =
    Either.cond(
      Ior
        .fromOptions(partyToParticipant.partySigningKeysWithThreshold, partyToKeyMappingO)
        // Validate that exactly one of them is defined
        .exists(!_.isBoth),
      (),
      "Party signing keys must be supplied either in the PartyToParticipant or in a PartyToKeyMapping transaction. Not in both.",
    )

  private def failOnUnwantedTransactionTypes(transactions: Seq[PositiveTopologyTransaction]) = {
    val unwantedTransactions =
      transactions.filterNot(tx =>
        expectedTransactionMappings.map(_.code).contains(tx.mapping.code)
      )
    Either.cond(
      unwantedTransactions.isEmpty,
      (),
      "Unsupported transactions found: " + unwantedTransactions.distinct
        .map(_.mapping.getClass.getSimpleName)
        .mkString(", ") + ". Supported transactions are: " + expectedTransactionMappings
        .map(_.getClass.getSimpleName.stripSuffix("$"))
        .mkString(", "),
    )
  }

  def create(
      signedTransactions: NonEmpty[List[(PositiveTopologyTransaction, List[Signature])]],
      multiSignatures: List[Signature],
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
  ): Either[String, ExternalPartyOnboardingDetails] =
    for {
      _ <- failOnUnwantedTransactionTypes(signedTransactions.map(_._1))
      parsedTransactionsWithSignatures <- parseTransactionsWithSignatures(
        signedTransactions,
        multiSignatures,
        protocolVersion,
      )
      (signedTopologyTransactions, unsignedTopologyTransactions) = parsedTransactionsWithSignatures
      partyToParticipantAndIsConfirming <- validatePartyToParticipant(
        signedTopologyTransactions,
        unsignedTopologyTransactions,
        participantId,
      )
      (partyToParticipant, isConfirming) = partyToParticipantAndIsConfirming
      partyToKey <- validatePartyToKey(
        signedTopologyTransactions,
        partyToParticipant.mapping.namespace,
      )
      _ <- validateExactlyOneSetOfSigningKeys(partyToParticipant.mapping, partyToKey.map(_.mapping))
      partyNamespace <- validatePartyNamespace(
        signedTopologyTransactions,
        partyToParticipant.mapping.namespace,
      )
    } yield ExternalPartyOnboardingDetails(
      partyNamespace,
      partyToKey,
      partyToParticipant,
      isConfirming,
    )
}
