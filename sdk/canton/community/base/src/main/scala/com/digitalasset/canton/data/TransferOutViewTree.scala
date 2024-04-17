// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.TransferOutMediatorMessage
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, LfWorkflowId, TransferCounter}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-out requests */
final case class TransferOutViewTree private (
    commonData: MerkleTreeLeaf[TransferOutCommonData],
    view: MerkleTree[TransferOutView],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TransferOutViewTree.type
    ],
    hashOps: HashOps,
) extends GenTransferViewTree[
      TransferOutCommonData,
      TransferOutView,
      TransferOutViewTree,
      TransferOutMediatorMessage,
    ](commonData, view)(hashOps)
    with HasRepresentativeProtocolVersion {

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferOutViewTree] = {

    if (
      optimizedBlindingPolicy.applyOrElse(
        commonData.rootHash,
        (_: RootHash) => RevealSubtree,
      ) != RevealSubtree
    )
      throw new IllegalArgumentException("Blinding of common data is not supported.")

    TransferOutViewTree(
      commonData,
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)
  }

  protected[this] override def createMediatorMessage(
      blindedTree: TransferOutViewTree,
      submittingParticipantSignature: Signature,
  ): TransferOutMediatorMessage =
    TransferOutMediatorMessage(blindedTree, submittingParticipantSignature)

  override def pretty: Pretty[TransferOutViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: TransferOutViewTree.type =
    TransferOutViewTree
}

object TransferOutViewTree
    extends HasProtocolVersionedWithContextAndValidationCompanion[
      TransferOutViewTree,
      HashOps,
    ] {

  override val name: String = "TransferOutViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      commonData: MerkleTreeLeaf[TransferOutCommonData],
      view: MerkleTree[TransferOutView],
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): TransferOutViewTree =
    TransferOutViewTree(commonData, view)(
      TransferOutViewTree.protocolVersionRepresentativeFor(protocolVersion),
      hashOps,
    )

  def fromProtoV30(context: (HashOps, ProtocolVersion))(
      transferOutViewTreeP: v30.TransferViewTree
  ): ParsingResult[TransferOutViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    val sourceProtocolVersion = SourceProtocolVersion(expectedProtocolVersion)

    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      res <- GenTransferViewTree.fromProtoV30(
        TransferOutCommonData.fromByteString(expectedProtocolVersion)(
          (hashOps, sourceProtocolVersion)
        ),
        TransferOutView.fromByteString(expectedProtocolVersion)(hashOps),
      )((commonData, view) =>
        TransferOutViewTree(commonData, view)(
          rpv,
          hashOps,
        )
      )(transferOutViewTreeP)
    } yield res

  }
}

/** Aggregates the data of a transfer-out request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param sourceDomain The domain to which the transfer-out request is sent
  * @param sourceMediator The mediator that coordinates the transfer-out request on the source domain
  * @param stakeholders The stakeholders of the contract to be transferred
  * @param adminParties The admin parties of transferring transfer-out participants
  * @param uuid The request UUID of the transfer-out
  * @param submitterMetadata information about the submission
  */
final case class TransferOutCommonData private (
    override val salt: Salt,
    sourceDomain: SourceDomainId,
    sourceMediator: MediatorGroupRecipient,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    uuid: UUID,
    submitterMetadata: TransferSubmitterMetadata,
)(
    hashOps: HashOps,
    val protocolVersion: SourceProtocolVersion,
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutCommonData](hashOps)
    with HasProtocolVersionedWrapper[TransferOutCommonData]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TransferOutCommonData.type =
    TransferOutCommonData

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[TransferOutCommonData.type] =
    TransferOutCommonData.protocolVersionRepresentativeFor(protocolVersion.v)

  protected def toProtoV30: v30.TransferOutCommonData =
    v30.TransferOutCommonData(
      salt = Some(salt.toProtoV30),
      sourceDomain = sourceDomain.toProtoPrimitive,
      sourceMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutCommonData

  def confirmingParties: Set[Informee] =
    (stakeholders ++ adminParties).map(ConfirmingParty(_, PositiveInt.one))

  override def pretty: Pretty[TransferOutCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("source domain", _.sourceDomain),
    param("source mediator", _.sourceMediator),
    param("stakeholders", _.stakeholders),
    param("admin parties", _.adminParties),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object TransferOutCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      TransferOutCommonData,
      (HashOps, SourceProtocolVersion),
    ] {
  override val name: String = "TransferOutCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceDomain: SourceDomainId,
      sourceMediator: MediatorGroupRecipient,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
      submitterMetadata: TransferSubmitterMetadata,
      protocolVersion: SourceProtocolVersion,
  ): TransferOutCommonData = TransferOutCommonData(
    salt,
    sourceDomain,
    sourceMediator,
    stakeholders,
    adminParties,
    uuid,
    submitterMetadata,
  )(hashOps, protocolVersion, None)

  private[this] def fromProtoV30(
      context: (HashOps, SourceProtocolVersion),
      transferOutCommonDataP: v30.TransferOutCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val (hashOps, sourceProtocolVersion) = context
    val v30.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      sourceMediatorP,
      submitterMetadataPO,
    ) = transferOutCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      sourceDomain <- SourceDomainId.fromProtoPrimitive(sourceDomainP, "source_domain")
      sourceMediator <- MediatorGroupRecipient.fromProtoPrimitive(
        sourceMediatorP,
        "source_mediator",
      )
      stakeholders <- stakeholdersP.traverse(ProtoConverter.parseLfPartyId)
      adminParties <- adminPartiesP.traverse(ProtoConverter.parseLfPartyId)
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      submitterMetadata <- ProtoConverter
        .required("submitter_metadata", submitterMetadataPO)
        .flatMap(TransferSubmitterMetadata.fromProtoV30)

    } yield TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders.toSet,
      adminParties.toSet,
      uuid,
      submitterMetadata,
    )(hashOps, sourceProtocolVersion, Some(bytes))
  }
}

/** Aggregates the data of a transfer-out request that is only sent to the involved participants
  */
/** @param salt The salt used to blind the Merkle hash.
  * @param submitterMetadata Metadata of the submitter
  * @param creatingTransactionId Id of the transaction that created the contract
  * @param contract Contract being transferred
  * @param targetDomain The domain to which the contract is transferred.
  * @param targetTimeProof The sequenced event from the target domain whose timestamp defines
  *                        the baseline for measuring time periods on the target domain
  * @param targetProtocolVersion Protocol version of the target domain
  */
final case class TransferOutView private (
    override val salt: Salt,
    creatingTransactionId: TransactionId,
    contract: SerializableContract,
    targetDomain: TargetDomainId,
    targetTimeProof: TimeProof,
    targetProtocolVersion: TargetProtocolVersion,
    transferCounter: TransferCounter,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferOutView](hashOps)
    with HasProtocolVersionedWrapper[TransferOutView]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: TransferOutView.type = TransferOutView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  def hashPurpose: HashPurpose = HashPurpose.TransferOutView

  def templateId: LfTemplateId =
    contract.rawContractInstance.contractInstance.unversioned.template

  protected def toProtoV30: v30.TransferOutView =
    v30.TransferOutView(
      salt = Some(salt.toProtoV30),
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV30),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
      transferCounter = transferCounter.toProtoPrimitive,
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      contract = Some(contract.toProtoV30),
    )

  override def pretty: Pretty[TransferOutView] = prettyOfClass(
    param("template id", _.templateId),
    param("creatingTransactionId", _.creatingTransactionId),
    param("contract", _.contract),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("target protocol version", _.targetProtocolVersion.v),
    param("salt", _.salt),
  )
}

object TransferOutView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferOutView, HashOps] {
  override val name: String = "TransferOutView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      creatingTransactionId: TransactionId,
      contract: SerializableContract,
      targetDomain: TargetDomainId,
      targetTimeProof: TimeProof,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
      transferCounter: TransferCounter,
  ): TransferOutView =
    TransferOutView(
      salt,
      creatingTransactionId,
      contract,
      targetDomain,
      targetTimeProof,
      targetProtocolVersion,
      transferCounter,
    )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.v), None)

  private[this] def fromProtoV30(hashOps: HashOps, transferOutViewP: v30.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutView] = {
    val v30.TransferOutView(
      saltP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
      transferCounter,
      creatingTransactionIdP,
      contractPO,
    ) = transferOutViewP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")
      targetProtocolVersion <- ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP)
      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV30(targetProtocolVersion, hashOps))
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      contract <- ProtoConverter
        .required("TransferOutViewTree.contract", contractPO)
        .flatMap(SerializableContract.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TransferOutView(
      salt,
      creatingTransactionId,
      contract,
      TargetDomainId(targetDomain),
      targetTimeProof,
      TargetProtocolVersion(targetProtocolVersion),
      TransferCounter(transferCounter),
    )(
      hashOps,
      rpv,
      Some(bytes),
    )

  }
}

/** A fully unblinded [[TransferOutViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullTransferOutTree(tree: TransferOutViewTree)
    extends TransferViewTree
    with HasVersionedToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "A transfer-out request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  def submitterMetadata: TransferSubmitterMetadata = commonData.submitterMetadata

  def submitter: LfPartyId = submitterMetadata.submitter
  def workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def adminParties: Set[LfPartyId] = commonData.adminParties

  def contractId: LfContractId = view.contract.contractId

  def templateId: LfTemplateId = view.templateId

  def transferCounter: TransferCounter = view.transferCounter

  def sourceDomain: SourceDomainId = commonData.sourceDomain

  def targetDomain: TargetDomainId = view.targetDomain

  def targetDomainPV: TargetProtocolVersion = view.targetProtocolVersion

  def targetTimeProof: TimeProof = view.targetTimeProof

  def mediatorMessage(submittingParticipantSignature: Signature): TransferOutMediatorMessage =
    tree.mediatorMessage(submittingParticipantSignature)

  override def domainId: DomainId = sourceDomain.unwrap

  override def mediator: MediatorGroupRecipient = commonData.sourceMediator

  override def informees: Set[Informee] = commonData.confirmingParties

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[FullTransferOutTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString(version: ProtocolVersion): ByteString = tree.toByteString(version)
}

object FullTransferOutTree {
  def fromByteString(
      crypto: CryptoPureApi,
      sourceProtocolVersion: SourceProtocolVersion,
  )(bytes: ByteString): ParsingResult[FullTransferOutTree] =
    for {
      tree <- TransferOutViewTree.fromByteString(crypto, sourceProtocolVersion.v)(bytes)
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-out request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferOutTree(tree)
}
