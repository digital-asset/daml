// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  TransferInMediatorMessage,
}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  NoOpeningErrors,
  SequencedEvent,
  SignedContent,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, LfWorkflowId, TransferCounter}
import com.google.protobuf.ByteString

import java.util.UUID

/** A transfer-in request embedded in a Merkle tree. The view may or may not be blinded. */
final case class TransferInViewTree(
    commonData: MerkleTreeLeaf[TransferInCommonData],
    view: MerkleTree[TransferInView],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransferInViewTree.type
    ],
    hashOps: HashOps,
) extends GenTransferViewTree[
      TransferInCommonData,
      TransferInView,
      TransferInViewTree,
      TransferInMediatorMessage,
    ](commonData, view)(hashOps)
    with HasProtocolVersionedWrapper[TransferInViewTree] {

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferInViewTree] = {

    if (
      optimizedBlindingPolicy.applyOrElse(
        commonData.rootHash,
        (_: RootHash) => RevealSubtree,
      ) != RevealSubtree
    )
      throw new IllegalArgumentException("Blinding of common data is not supported.")

    TransferInViewTree(
      commonData,
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)
  }

  protected[this] override def createMediatorMessage(
      blindedTree: TransferInViewTree,
      submittingParticipantSignature: Signature,
  ): TransferInMediatorMessage =
    TransferInMediatorMessage(blindedTree, submittingParticipantSignature)

  override def pretty: Pretty[TransferInViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: TransferInViewTree.type =
    TransferInViewTree
}

object TransferInViewTree
    extends HasProtocolVersionedWithContextAndValidationWithTargetProtocolVersionCompanion[
      TransferInViewTree,
      HashOps,
    ] {

  override val name: String = "TransferInViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      commonData: MerkleTreeLeaf[TransferInCommonData],
      view: MerkleTree[TransferInView],
      targetProtocolVersion: TargetProtocolVersion,
      hashOps: HashOps,
  ): TransferInViewTree =
    TransferInViewTree(commonData, view)(
      TransferInViewTree.protocolVersionRepresentativeFor(targetProtocolVersion.v),
      hashOps,
    )

  def fromProtoV30(context: (HashOps, TargetProtocolVersion))(
      transferInViewTreeP: v30.TransferViewTree
  ): ParsingResult[TransferInViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      res <- GenTransferViewTree.fromProtoV30(
        TransferInCommonData.fromByteString(expectedProtocolVersion.v)(
          (hashOps, expectedProtocolVersion)
        ),
        TransferInView.fromByteString(expectedProtocolVersion.v)(hashOps),
      )((commonData, view) =>
        TransferInViewTree(commonData, view)(
          rpv,
          hashOps,
        )
      )(transferInViewTreeP)
    } yield res
  }
}

/** Aggregates the data of a transfer-in request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param targetDomain The domain on which the contract is transferred in
  * @param targetMediator The mediator that coordinates the transfer-in request on the target domain
  * @param stakeholders The stakeholders of the transferred contract
  * @param uuid The uuid of the transfer-in request
  * @param submitterMetadata information about the submission
  */
final case class TransferInCommonData private (
    override val salt: Salt,
    targetDomain: TargetDomainId,
    targetMediator: MediatorGroupRecipient,
    stakeholders: Set[LfPartyId],
    uuid: UUID,
    submitterMetadata: TransferSubmitterMetadata,
)(
    hashOps: HashOps,
    val targetProtocolVersion: TargetProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInCommonData](hashOps)
    with HasProtocolVersionedWrapper[TransferInCommonData]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TransferInCommonData.type =
    TransferInCommonData

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferInCommonData.type] =
    TransferInCommonData.protocolVersionRepresentativeFor(targetProtocolVersion.v)

  protected def toProtoV30: v30.TransferInCommonData =
    v30.TransferInCommonData(
      salt = Some(salt.toProtoV30),
      targetDomain = targetDomain.toProtoPrimitive,
      targetMediator = targetMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferInCommonData

  def confirmingParties: Set[Informee] =
    stakeholders.map(ConfirmingParty(_, PositiveInt.one))

  override def pretty: Pretty[TransferInCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("target domain", _.targetDomain),
    param("target mediator", _.targetMediator),
    param("stakeholders", _.stakeholders),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object TransferInCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransferInCommonData,
      (HashOps, TargetProtocolVersion),
    ] {
  override val name: String = "TransferInCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferInCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: TargetDomainId,
      targetMediator: MediatorGroupRecipient,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
      submitterMetadata: TransferSubmitterMetadata,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferInCommonData = TransferInCommonData(
    salt,
    targetDomain,
    targetMediator,
    stakeholders,
    uuid,
    submitterMetadata,
  )(hashOps, targetProtocolVersion, None)

  private[this] def fromProtoV30(
      context: (HashOps, TargetProtocolVersion),
      transferInCommonDataP: v30.TransferInCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val (hashOps, targetProtocolVersion) = context
    val v30.TransferInCommonData(
      saltP,
      targetDomainP,
      stakeholdersP,
      uuidP,
      targetMediatorP,
      submitterMetadataPO,
    ) = transferInCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetDomain <- TargetDomainId.fromProtoPrimitive(targetDomainP, "target_domain")
      targetMediator <- MediatorGroupRecipient.fromProtoPrimitive(
        targetMediatorP,
        "target_mediator",
      )
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      submitterMetadata <- ProtoConverter
        .required("submitter_metadata", submitterMetadataPO)
        .flatMap(TransferSubmitterMetadata.fromProtoV30)

    } yield TransferInCommonData(
      salt,
      targetDomain,
      targetMediator,
      stakeholders.toSet,
      uuid,
      submitterMetadata,
    )(hashOps, targetProtocolVersion, Some(bytes))
  }
}

/** Aggregates the data of a transfer-in request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param contract The contract to be transferred including the instance
  * @param creatingTransactionId The id of the transaction that created the contract
  * @param transferOutResultEvent The signed deliver event of the transfer-out result message
  * @param sourceProtocolVersion Protocol version of the source domain.
  * @param transferCounter The [[com.digitalasset.canton.TransferCounter]] of the contract.
  */
final case class TransferInView private (
    override val salt: Salt,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResultEvent: DeliveredTransferOutResult,
    sourceProtocolVersion: SourceProtocolVersion,
    transferCounter: TransferCounter,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInView](hashOps)
    with HasProtocolVersionedWrapper[TransferInView]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TransferInView.type = TransferInView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  def hashPurpose: HashPurpose = HashPurpose.TransferInView

  protected def toProtoV30: v30.TransferInView =
    v30.TransferInView(
      salt = Some(salt.toProtoV30),
      contract = Some(contract.toProtoV30),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = transferOutResultEvent.result.toByteString,
      sourceProtocolVersion = sourceProtocolVersion.v.toProtoPrimitive,
      transferCounter = transferCounter.toProtoPrimitive,
    )

  override def pretty: Pretty[TransferInView] = prettyOfClass(
    param("creating transaction id", _.creatingTransactionId),
    param("transfer out result event", _.transferOutResultEvent),
    param("source protocol version", _.sourceProtocolVersion.v),
    param("transfer counter", _.transferCounter),
    param(
      "contract id",
      _.contract.contractId,
    ), // do not log contract details because it contains confidential data
    param("salt", _.salt),
  )
}

object TransferInView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInView, HashOps] {
  override val name: String = "TransferInView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferInView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
      transferCounter: TransferCounter,
  ): Either[String, TransferInView] = Either
    .catchOnly[IllegalArgumentException](
      TransferInView(
        salt,
        contract,
        creatingTransactionId,
        transferOutResultEvent,
        sourceProtocolVersion,
        transferCounter,
      )(hashOps, protocolVersionRepresentativeFor(targetProtocolVersion.v), None)
    )
    .leftMap(_.getMessage)

  private[this] def fromProtoV30(hashOps: HashOps, transferInViewP: v30.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v30.TransferInView(
      saltP,
      contractP,
      transferOutResultEventP,
      creatingTransactionIdP,
      sourceProtocolVersionP,
      transferCounterP,
    ) =
      transferInViewP
    for {
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(sourceProtocolVersionP)
      sourceProtocolVersion = SourceProtocolVersion(protocolVersion)
      commonData <- CommonData.fromProto(
        hashOps,
        saltP,
        transferOutResultEventP,
        creatingTransactionIdP,
        sourceProtocolVersion,
      )
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TransferInView(
      commonData.salt,
      contract,
      commonData.creatingTransactionId,
      commonData.transferOutResultEvent,
      commonData.sourceProtocolVersion,
      TransferCounter(transferCounterP),
    )(hashOps, rpv, Some(bytes))
  }

  private[TransferInView] final case class CommonData(
      salt: Salt,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      sourceProtocolVersion: SourceProtocolVersion,
  )

  private[TransferInView] object CommonData {
    def fromProto(
        hashOps: HashOps,
        saltP: Option[com.digitalasset.canton.crypto.v30.Salt],
        transferOutResultEventP: ByteString,
        creatingTransactionIdP: ByteString,
        sourceProtocolVersion: SourceProtocolVersion,
    ): ParsingResult[CommonData] = {
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
        // TransferOutResultEvent deserialization
        transferOutResultEventMC <- SignedContent
          .fromByteString(sourceProtocolVersion.v)(transferOutResultEventP)
          .flatMap(
            _.deserializeContent(
              SequencedEvent.fromByteStringOpen(hashOps, sourceProtocolVersion.v)
            )
          )
        transferOutResultEvent <- DeliveredTransferOutResult
          .create(NoOpeningErrors(transferOutResultEventMC))
          .leftMap(err => OtherError(err.toString))
        creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      } yield CommonData(
        salt,
        creatingTransactionId,
        transferOutResultEvent,
        sourceProtocolVersion,
      )
    }
  }
}

/** A fully unblinded [[TransferInViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullTransferInTree(tree: TransferInViewTree)
    extends TransferViewTree
    with HasToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-in request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  def submitterMetadata: TransferSubmitterMetadata = commonData.submitterMetadata

  def submitter: LfPartyId = submitterMetadata.submitter

  def workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def contract: SerializableContract = view.contract

  def transferCounter: TransferCounter = view.transferCounter

  def creatingTransactionId: TransactionId = view.creatingTransactionId

  def transferOutResultEvent: DeliveredTransferOutResult = view.transferOutResultEvent

  def mediatorMessage(
      submittingParticipantSignature: Signature
  ): TransferInMediatorMessage = tree.mediatorMessage(submittingParticipantSignature)

  def targetDomain: TargetDomainId = commonData.targetDomain

  override def domainId: DomainId = commonData.targetDomain.unwrap

  override def mediator: MediatorGroupRecipient = commonData.targetMediator

  override def informees: Set[LfPartyId] = commonData.confirmingParties.map(_.party)

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override def isTransferringParticipant(participantId: ParticipantId): Boolean =
    transferOutResultEvent.unwrap.informees.contains(participantId.adminParty.toLf)

  override def pretty: Pretty[FullTransferInTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString: ByteString = tree.toByteString
}

object FullTransferInTree {
  def fromByteString(
      crypto: CryptoPureApi,
      targetProtocolVersion: TargetProtocolVersion,
  )(bytes: ByteString): ParsingResult[FullTransferInTree] =
    for {
      tree <- TransferInViewTree.fromByteString(crypto, targetProtocolVersion)(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-in request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferInTree(tree)
}
