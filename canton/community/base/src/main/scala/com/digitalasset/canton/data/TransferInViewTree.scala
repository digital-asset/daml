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
import com.digitalasset.canton.sequencing.protocol.{SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{DomainId, MediatorRef, ParticipantId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, LfWorkflowId, TransferCounter, TransferCounterO}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-in requests */

final case class TransferInViewTree(
    commonData: MerkleTreeLeaf[TransferInCommonData],
    view: MerkleTree[TransferInView],
)(hashOps: HashOps)
    extends GenTransferViewTree[
      TransferInCommonData,
      TransferInView,
      TransferInViewTree,
      TransferInMediatorMessage,
    ](commonData, view)(hashOps) {

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant

  override def createMediatorMessage(
      blindedTree: TransferInViewTree,
      submittingParticipantSignature: Signature,
  ): TransferInMediatorMessage =
    TransferInMediatorMessage(blindedTree, submittingParticipantSignature)

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
    )(hashOps)
  }

  override def pretty: Pretty[TransferInViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )
}

object TransferInViewTree
    extends HasVersionedMessageWithContextCompanion[
      TransferInViewTree,
      (HashOps, ProtocolVersion),
    ] {
  override val name: String = "TransferInViewTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.TransferViewTree)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      transferInViewTreeP: v30.TransferViewTree,
  ): ParsingResult[TransferInViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    GenTransferViewTree.fromProtoV30(
      TransferInCommonData.fromByteString(expectedProtocolVersion)(hashOps),
      TransferInView.fromByteString(expectedProtocolVersion)(hashOps),
    )((commonData, view) => new TransferInViewTree(commonData, view)(hashOps))(
      transferInViewTreeP
    )
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
    targetMediator: MediatorRef,
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

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferInCommonData.type] =
    TransferInCommonData.protocolVersionRepresentativeFor(targetProtocolVersion.v)

  def confirmingParties: Set[Informee] =
    stakeholders.map(ConfirmingParty(_, PositiveInt.one, TrustLevel.Ordinary))

  @transient override protected lazy val companionObj: TransferInCommonData.type =
    TransferInCommonData

  protected def toProtoV30: v30.TransferInCommonData =
    v30.TransferInCommonData(
      salt = Some(salt.toProtoV30),
      targetDomain = targetDomain.toProtoPrimitive,
      targetMediator = targetMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
    )

  override def hashPurpose: HashPurpose = HashPurpose.TransferInCommonData

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

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
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInCommonData, HashOps] {
  override val name: String = "TransferInCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.TransferInCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: TargetDomainId,
      targetMediator: MediatorRef,
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

  private[this] def fromProtoV30(hashOps: HashOps, transferInCommonDataP: v30.TransferInCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val v30.TransferInCommonData(
      saltP,
      targetDomainP,
      stakeholdersP,
      uuidP,
      targetMediatorP,
      protocolVersionP,
      submitterMetadataPO,
    ) =
      transferInCommonDataP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetDomain <- TargetDomainId.fromProtoPrimitive(targetDomainP, "target_domain")
      targetMediator <- MediatorRef.fromProtoPrimitive(targetMediatorP, "target_mediator")
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
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
    )(
      hashOps,
      TargetProtocolVersion(protocolVersion),
      Some(bytes),
    )
  }
}

// TODO(#15159) For transfer counter, remove the note that it is defined iff...
/** Aggregates the data of a transfer-in request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param submitter The submitter of the transfer-in request
  * @param creatingTransactionId The id of the transaction that created the contract
  * @param contract The contract to be transferred including the instance
  * @param transferOutResultEvent The signed deliver event of the transfer-out result message
  * @param transferCounter The [[com.digitalasset.canton.TransferCounter]] of the contract.
  */
final case class TransferInView private (
    override val salt: Salt,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResultEvent: DeliveredTransferOutResult,
    sourceProtocolVersion: SourceProtocolVersion,
    // TODO(#15179) Remove the option
    transferCounter: TransferCounterO,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInView](hashOps)
    with HasProtocolVersionedWrapper[TransferInView]
    with ProtocolVersionedMemoizedEvidence {

  override def hashPurpose: HashPurpose = HashPurpose.TransferInView

  @transient override protected lazy val companionObj: TransferInView.type = TransferInView

  protected def toProtoV30: v30.TransferInView =
    v30.TransferInView(
      salt = Some(salt.toProtoV30),
      contract = Some(contract.toProtoV30),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV30),
      sourceProtocolVersion = sourceProtocolVersion.v.toProtoPrimitive,
      transferCounter = transferCounter
        .getOrElse(
          throw new IllegalStateException(
            s"Transfer counter must be defined at representative protocol version $representativeProtocolVersion"
          )
        )
        .toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferInView] = prettyOfClass(
    param("contract", _.contract), // TODO(#3269) this may contain confidential data
    paramIfDefined("transfer counter", _.transferCounter),
    param("creating transaction id", _.creatingTransactionId),
    param("transfer out result", _.transferOutResultEvent),
    param("salt", _.salt),
  )
}

object TransferInView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInView, HashOps] {
  override val name: String = "TransferInView"

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
        transferOutResultEventPO: Option[v30.SignedContent],
        creatingTransactionIdP: ByteString,
        sourceProtocolVersion: ProtocolVersion,
    ): ParsingResult[CommonData] = {
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
        // TransferOutResultEvent deserialization
        transferOutResultEventP <- ProtoConverter
          .required("TransferInView.transferOutResultEvent", transferOutResultEventPO)

        transferOutResultEventMC <- SignedContent
          .fromProtoV30(transferOutResultEventP)
          .flatMap(
            _.deserializeContent(SequencedEvent.fromByteStringOpen(hashOps, sourceProtocolVersion))
          )
        transferOutResultEvent <- DeliveredTransferOutResult
          .create(Right(transferOutResultEventMC))
          .leftMap(err => OtherError(err.toString))
        creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      } yield CommonData(
        salt,
        creatingTransactionId,
        transferOutResultEvent,
        SourceProtocolVersion(sourceProtocolVersion),
      )
    }
  }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.TransferInView)(
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
      transferCounter: TransferCounterO,
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
      transferOutResultEventPO,
      creatingTransactionIdP,
      sourceProtocolVersionP,
      transferCounterP,
    ) =
      transferInViewP
    for {
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(sourceProtocolVersionP)
      commonData <- CommonData.fromProto(
        hashOps,
        saltP,
        transferOutResultEventPO,
        creatingTransactionIdP,
        protocolVersion,
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
      Some(TransferCounter(transferCounterP)),
    )(hashOps, rpv, Some(bytes))
  }
}

/** A fully unblinded [[TransferInViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullTransferInTree(tree: TransferInViewTree)
    extends TransferViewTree
    with HasVersionedToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-in request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  def submitterMetadata: TransferSubmitterMetadata = commonData.submitterMetadata

  def submitter: LfPartyId = submitterMetadata.submitter

  def workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def contract: SerializableContract = view.contract

  def transferCounter: TransferCounterO = view.transferCounter

  def creatingTransactionId: TransactionId = view.creatingTransactionId

  def transferOutResultEvent: DeliveredTransferOutResult = view.transferOutResultEvent

  def mediatorMessage(
      submittingParticipantSignature: Signature
  ): TransferInMediatorMessage = tree.mediatorMessage(submittingParticipantSignature)

  override def domainId: DomainId = commonData.targetDomain.unwrap

  def targetDomain: TargetDomainId = commonData.targetDomain

  override def mediator: MediatorRef = commonData.targetMediator

  override def informees: Set[Informee] = commonData.confirmingParties

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def toByteString(version: ProtocolVersion): ByteString = tree.toByteString(version)

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[FullTransferInTree] = prettyOfClass(unnamedParam(_.tree))
}

object FullTransferInTree {
  def fromByteString(
      crypto: CryptoPureApi,
      expectedProtocolVersion: ProtocolVersion,
  )(bytes: ByteString): ParsingResult[FullTransferInTree] =
    for {
      tree <- TransferInViewTree.fromByteString((crypto, expectedProtocolVersion))(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-in request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferInTree(tree)
}
