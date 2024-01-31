// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  DeliveredTransferOutResult,
  TransferInMediatorMessage,
}
import com.digitalasset.canton.protocol.{
  RootHash,
  SerializableContract,
  TargetDomainId,
  TransactionId,
  ViewHash,
  v0,
  v1,
}
import com.digitalasset.canton.sequencing.protocol.{SequencedEvent, SignedContent}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWithContextCompanion,
  HasProtocolVersionedWrapper,
  HasVersionedMessageWithContextCompanion,
  HasVersionedToByteString,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerCommandId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-in requests */

final case class TransferInViewTree(
    commonData: MerkleTree[TransferInCommonData],
    view: MerkleTree[TransferInView],
)(hashOps: HashOps)
    extends GenTransferViewTree[
      TransferInCommonData,
      TransferInView,
      TransferInViewTree,
      TransferInMediatorMessage,
    ](commonData, view)(hashOps) {

  override def createMediatorMessage(blindedTree: TransferInViewTree): TransferInMediatorMessage =
    TransferInMediatorMessage(blindedTree)

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferInViewTree] =
    TransferInViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(hashOps)

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
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.TransferViewTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.TransferViewTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def fromProtoV0(
      context: (HashOps, ProtocolVersion),
      transferInViewTreeP: v0.TransferViewTree,
  ): ParsingResult[TransferInViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    GenTransferViewTree.fromProtoV0(
      TransferInCommonData.fromByteString(expectedProtocolVersion)(hashOps),
      TransferInView.fromByteString(expectedProtocolVersion)(hashOps),
    )((commonData, view) => new TransferInViewTree(commonData, view)(hashOps))(
      transferInViewTreeP
    )
  }

  def fromProtoV1(
      context: (HashOps, ProtocolVersion),
      transferInViewTreeP: v1.TransferViewTree,
  ): ParsingResult[TransferInViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    GenTransferViewTree.fromProtoV1(
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
  */
final case class TransferInCommonData private (
    override val salt: Salt,
    targetDomain: TargetDomainId,
    targetMediator: MediatorRef,
    stakeholders: Set[LfPartyId],
    uuid: UUID,
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

  protected def toProtoV0: v0.TransferInCommonData =
    v0.TransferInCommonData(
      salt = Some(salt.toProtoV0),
      targetDomain = targetDomain.toProtoPrimitive,
      targetMediator = targetMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    )

  protected def toProtoV1: v1.TransferInCommonData =
    v1.TransferInCommonData(
      salt = Some(salt.toProtoV0),
      targetDomain = targetDomain.toProtoPrimitive,
      targetMediator = targetMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
    )

  override def hashPurpose: HashPurpose = HashPurpose.TransferInCommonData

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferInCommonData] = prettyOfClass(
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
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferInCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferInCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: TargetDomainId,
      targetMediator: MediatorRef,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
      targetProtocolVersion: TargetProtocolVersion,
  ): TransferInCommonData = TransferInCommonData(
    salt,
    targetDomain,
    targetMediator,
    stakeholders,
    uuid,
  )(hashOps, targetProtocolVersion, None)

  private[this] def fromProtoV0(hashOps: HashOps, transferInCommonDataP: v0.TransferInCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val v0.TransferInCommonData(saltP, targetDomainP, stakeholdersP, uuidP, targetMediatorP) =
      transferInCommonDataP
    for {
      commonData <- ParsedDataV0V1.fromProto(
        saltP,
        targetDomainP,
        stakeholdersP,
        uuidP,
        targetMediatorP,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
    } yield TransferInCommonData(
      commonData.salt,
      commonData.targetDomain,
      commonData.targetMediator,
      commonData.stakeholders,
      commonData.uuid,
    )(
      hashOps,
      TargetProtocolVersion(rpv.representative),
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferInCommonDataP: v1.TransferInCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferInCommonData] = {
    val v1.TransferInCommonData(
      saltP,
      targetDomainP,
      stakeholdersP,
      uuidP,
      targetMediatorP,
      protocolVersionP,
    ) =
      transferInCommonDataP
    for {
      commonData <- ParsedDataV0V1.fromProto(
        saltP,
        targetDomainP,
        stakeholdersP,
        uuidP,
        targetMediatorP,
      )
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
    } yield TransferInCommonData(
      commonData.salt,
      commonData.targetDomain,
      commonData.targetMediator,
      commonData.stakeholders,
      commonData.uuid,
    )(
      hashOps,
      TargetProtocolVersion(protocolVersion),
      Some(bytes),
    )
  }

  final case class ParsedDataV0V1(
      salt: Salt,
      targetDomain: TargetDomainId,
      targetMediator: MediatorRef,
      stakeholders: Set[LfPartyId],
      uuid: UUID,
  )

  private[TransferInCommonData] object ParsedDataV0V1 {
    def fromProto(
        salt: Option[com.digitalasset.canton.crypto.v0.Salt],
        targetDomain: String,
        stakeholders: Seq[String],
        uuid: String,
        targetMediator: String,
    ): ParsingResult[ParsedDataV0V1] =
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", salt)
        targetDomain <- DomainId.fromProtoPrimitive(targetDomain, "target_domain")
        targetMediator <- MediatorRef.fromProtoPrimitive(targetMediator, "target_mediator")
        stakeholders <- stakeholders.traverse(ProtoConverter.parseLfPartyId)
        uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuid)
      } yield ParsedDataV0V1(
        salt,
        TargetDomainId(targetDomain),
        targetMediator,
        stakeholders.toSet,
        uuid,
      )
  }
}

/** Aggregates the data of a transfer-in request that is only sent to the involved participants
  *
  * @param salt The salt to blind the Merkle hash
  * @param submitterMetadata The submitter of the transfer-in request
  * @param creatingTransactionId The id of the transaction that created the contract
  * @param contract The contract to be transferred including the instance
  * @param transferOutResultEvent The signed deliver event of the transfer-out result message
  */
final case class TransferInView private (
    override val salt: Salt,
    submitterMetadata: TransferSubmitterMetadata,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResultEvent: DeliveredTransferOutResult,
    sourceProtocolVersion: SourceProtocolVersion,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferInView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[TransferInView](hashOps)
    with HasProtocolVersionedWrapper[TransferInView]
    with ProtocolVersionedMemoizedEvidence {

  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw new IllegalArgumentException(err))

  override def hashPurpose: HashPurpose = HashPurpose.TransferInView

  @transient override protected lazy val companionObj: TransferInView.type = TransferInView

  val submitter: LfPartyId = submitterMetadata.submitter
  val submittingParticipant: LedgerParticipantId = submitterMetadata.submittingParticipant
  val applicationId: LedgerApplicationId = submitterMetadata.applicationId
  val submissionId: Option[LedgerSubmissionId] = submitterMetadata.submissionId
  val commandId: LedgerCommandId = submitterMetadata.commandId
  val workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  protected def toProtoV0: v0.TransferInView =
    v0.TransferInView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contract = Some(contract.toProtoV0),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV0),
    )

  protected def toProtoV1: v1.TransferInView =
    v1.TransferInView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contract = Some(contract.toProtoV1),
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      transferOutResultEvent = Some(transferOutResultEvent.result.toProtoV0),
      sourceProtocolVersion = sourceProtocolVersion.v.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def pretty: Pretty[TransferInView] = prettyOfClass(
    param("submitter", _.submitter),
    param("contract", _.contract), // TODO(#3269) this may contain confidential data
    param("creating transaction id", _.creatingTransactionId),
    param("transfer out result", _.transferOutResultEvent),
    param("submitting participant", _.submittingParticipant),
    param("application id", _.applicationId),
    paramIfDefined("submission id", _.submissionId),
    paramIfDefined("workflow id", _.workflowId),
    param("salt", _.salt),
  )
}

object TransferInView
    extends HasMemoizedProtocolVersionedWithContextCompanion[TransferInView, HashOps] {
  override val name: String = "TransferInView"

  private[TransferInView] final case class CommonData(
      salt: Salt,
      submitter: LfPartyId,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      sourceProtocolVersion: SourceProtocolVersion,
  )

  private[TransferInView] object CommonData {
    def fromProto(
        hashOps: HashOps,
        saltP: Option[com.digitalasset.canton.crypto.v0.Salt],
        submitterP: String,
        transferOutResultEventPO: Option[v0.SignedContent],
        creatingTransactionIdP: ByteString,
        sourceProtocolVersion: ProtocolVersion,
    ): ParsingResult[CommonData] = {
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
        submitter <- ProtoConverter.parseLfPartyId(submitterP)
        // TransferOutResultEvent deserialization
        transferOutResultEventP <- ProtoConverter
          .required("TransferInView.transferOutResultEvent", transferOutResultEventPO)

        transferOutResultEventMC <- SignedContent
          .fromProtoV0(transferOutResultEventP)
          .flatMap(
            _.deserializeContent(SequencedEvent.fromByteStringOpen(hashOps, sourceProtocolVersion))
          )
        transferOutResultEvent <- DeliveredTransferOutResult
          .create(Right(transferOutResultEventMC))
          .leftMap(err => OtherError(err.toString))
        creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      } yield CommonData(
        salt,
        submitter,
        creatingTransactionId,
        transferOutResultEvent,
        SourceProtocolVersion(sourceProtocolVersion),
      )
    }
  }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferInView)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferInView)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override lazy val invariants = Seq(
    sourceProtocolVersionDefaultValue
  )

  private lazy val rpv4: RepresentativeProtocolVersion[TransferInView.type] =
    protocolVersionRepresentativeFor(ProtocolVersion.v4)

  lazy val submittingParticipantDefaultValue: LedgerParticipantId =
    LedgerParticipantId.assertFromString("no-participant-id")

  lazy val commandIdDefaultValue: LedgerCommandId =
    LedgerCommandId.assertFromString("no-command-id")

  lazy val applicationIdDefaultValue: LedgerApplicationId =
    LedgerApplicationId.assertFromString("no-application-id")

  lazy val submissionIdDefaultValue: Option[LedgerSubmissionId] = None

  lazy val workflowIdDefaultValue: Option[LfWorkflowId] = None

  lazy val sourceProtocolVersionDefaultValue: DefaultValueUntilExclusive[SourceProtocolVersion] =
    DefaultValueUntilExclusive(
      _.sourceProtocolVersion,
      "sourceProtocolVersion",
      rpv4,
      SourceProtocolVersion(ProtocolVersion.v3),
    )

  def create(hashOps: HashOps)(
      salt: Salt,
      submitterMetadata: TransferSubmitterMetadata,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      transferOutResultEvent: DeliveredTransferOutResult,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
  ): Either[String, TransferInView] = Either
    .catchOnly[IllegalArgumentException](
      TransferInView(
        salt,
        submitterMetadata,
        contract,
        creatingTransactionId,
        transferOutResultEvent,
        sourceProtocolVersion,
      )(hashOps, protocolVersionRepresentativeFor(targetProtocolVersion.v), None)
    )
    .leftMap(_.getMessage)

  private[this] def fromProtoV0(hashOps: HashOps, transferInViewP: v0.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v0.TransferInView(
      saltP,
      submitterP,
      contractP,
      transferOutResultEventPO,
      creatingTransactionIdP,
    ) =
      transferInViewP
    for {
      commonData <- CommonData.fromProto(
        hashOps,
        saltP,
        submitterP,
        transferOutResultEventPO,
        creatingTransactionIdP,
        ProtocolVersion.v3,
      )
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV0)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
    } yield TransferInView(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        applicationIdDefaultValue,
        submittingParticipantDefaultValue,
        commandIdDefaultValue,
        submissionId = submissionIdDefaultValue,
        workflowId = None,
      ),
      contract,
      commonData.creatingTransactionId,
      commonData.transferOutResultEvent,
      commonData.sourceProtocolVersion,
    )(hashOps, rpv, Some(bytes))
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferInViewP: v1.TransferInView)(
      bytes: ByteString
  ): ParsingResult[TransferInView] = {
    val v1.TransferInView(
      saltP,
      submitterP,
      contractP,
      transferOutResultEventPO,
      creatingTransactionIdP,
      sourceProtocolVersionP,
    ) =
      transferInViewP
    for {
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(sourceProtocolVersionP)
      commonData <- CommonData.fromProto(
        hashOps,
        saltP,
        submitterP,
        transferOutResultEventPO,
        creatingTransactionIdP,
        protocolVersion,
      )
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV1)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(1))
    } yield TransferInView(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        applicationIdDefaultValue,
        submittingParticipantDefaultValue,
        commandIdDefaultValue,
        submissionId = submissionIdDefaultValue,
        workflowId = None,
      ),
      contract,
      commonData.creatingTransactionId,
      commonData.transferOutResultEvent,
      commonData.sourceProtocolVersion,
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

  def submitter: LfPartyId = view.submitter

  def submitterMetadata: TransferSubmitterMetadata = view.submitterMetadata

  def workflowId: Option[LfWorkflowId] = view.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def contract: SerializableContract = view.contract

  def creatingTransactionId: TransactionId = view.creatingTransactionId

  def transferOutResultEvent: DeliveredTransferOutResult = view.transferOutResultEvent

  def mediatorMessage: TransferInMediatorMessage = tree.mediatorMessage

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
