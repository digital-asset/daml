// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.messages.TransferOutMediatorMessage
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.topology.{DomainId, MediatorRef}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerCommandId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
  ProtoDeserializationError,
  TransferCounter,
  TransferCounterO,
}
import com.google.protobuf.ByteString

import java.util.UUID

/** A blindable Merkle tree for transfer-out requests */
final case class TransferOutViewTree private (
    commonData: MerkleTree[TransferOutCommonData],
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

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransferOutViewTree] =
    TransferOutViewTree(
      commonData.doBlind(optimizedBlindingPolicy),
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)

  protected[this] override def createMediatorMessage(
      blindedTree: TransferOutViewTree
  ): TransferOutMediatorMessage =
    TransferOutMediatorMessage(blindedTree)

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
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV0(context)(proto)),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV1(context)(proto)),
      _.toProtoV1.toByteString,
    ),
  )

  def apply(
      commonData: MerkleTree[TransferOutCommonData],
      view: MerkleTree[TransferOutView],
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  ): TransferOutViewTree =
    TransferOutViewTree(commonData, view)(
      TransferOutViewTree.protocolVersionRepresentativeFor(protocolVersion),
      hashOps,
    )

  def fromProtoV0(context: (HashOps, ProtocolVersion))(
      transferOutViewTreeP: v0.TransferViewTree
  ): ParsingResult[TransferOutViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    GenTransferViewTree.fromProtoV0(
      TransferOutCommonData.fromByteString(expectedProtocolVersion)(hashOps),
      TransferOutView.fromByteString(expectedProtocolVersion)(hashOps),
    )((commonData, view) =>
      TransferOutViewTree(commonData, view)(
        protocolVersionRepresentativeFor(ProtoVersion(0)),
        hashOps,
      )
    )(transferOutViewTreeP)
  }

  def fromProtoV1(context: (HashOps, ProtocolVersion))(
      transferOutViewTreeP: v1.TransferViewTree
  ): ParsingResult[TransferOutViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    GenTransferViewTree.fromProtoV1(
      TransferOutCommonData.fromByteString(expectedProtocolVersion)(hashOps),
      TransferOutView.fromByteString(expectedProtocolVersion)(hashOps),
    )((commonData, view) =>
      TransferOutViewTree(commonData, view)(
        protocolVersionRepresentativeFor(ProtoVersion(1)),
        hashOps,
      )
    )(transferOutViewTreeP)
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
  */
final case class TransferOutCommonData private (
    override val salt: Salt,
    sourceDomain: SourceDomainId,
    sourceMediator: MediatorRef,
    stakeholders: Set[LfPartyId],
    adminParties: Set[LfPartyId],
    uuid: UUID,
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

  protected def toProtoV0: v0.TransferOutCommonData =
    v0.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      originDomain = sourceDomain.toProtoPrimitive,
      originMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    )

  protected def toProtoV1: v1.TransferOutCommonData =
    v1.TransferOutCommonData(
      salt = Some(salt.toProtoV0),
      sourceDomain = sourceDomain.toProtoPrimitive,
      sourceMediator = sourceMediator.toProtoPrimitive,
      stakeholders = stakeholders.toSeq,
      adminParties = adminParties.toSeq,
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      sourceProtocolVersion = protocolVersion.v.toProtoPrimitive,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.TransferOutCommonData

  def confirmingParties: Set[Informee] =
    (stakeholders ++ adminParties).map(ConfirmingParty(_, PositiveInt.one, TrustLevel.Ordinary))

  override def pretty: Pretty[TransferOutCommonData] = prettyOfClass(
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
      HashOps,
    ] {
  override val name: String = "TransferOutCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferOutCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceDomain: SourceDomainId,
      sourceMediator: MediatorRef,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
      protocolVersion: SourceProtocolVersion,
  ): Either[String, TransferOutCommonData] = {
    for {
      _ <- TransferCommonData.checkMediatorGroup(sourceMediator, protocolVersion.v)
    } yield TransferOutCommonData(
      salt,
      sourceDomain,
      sourceMediator,
      stakeholders,
      adminParties,
      uuid,
    )(hashOps, protocolVersion, None)
  }

  private[this] def checkMediatorGroupForProtocolVersion(
      commonData: ParsedDataV0V1,
      protocolVersion: ProtocolVersion,
  ): Either[ProtoDeserializationError.InvariantViolation, Unit] =
    TransferCommonData
      .checkMediatorGroup(commonData.sourceMediator, protocolVersion)
      .leftMap(ProtoDeserializationError.InvariantViolation.apply)

  private[this] def fromProtoV0(hashOps: HashOps, transferOutCommonDataP: v0.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v0.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
    ) = transferOutCommonDataP
    for {
      commonData <- ParsedDataV0V1.fromProto(
        saltP,
        sourceDomainP,
        mediatorIdP,
        stakeholdersP,
        adminPartiesP,
        uuidP,
      )
      _ <- checkMediatorGroupForProtocolVersion(commonData, ProtocolVersion.v3)
    } yield TransferOutCommonData(
      commonData.salt,
      commonData.sourceDomain,
      commonData.sourceMediator,
      commonData.stakeholders,
      commonData.adminParties,
      commonData.uuid,
    )(
      hashOps,
      SourceProtocolVersion(protocolVersionRepresentativeFor(ProtoVersion(0)).representative),
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutCommonDataP: v1.TransferOutCommonData)(
      bytes: ByteString
  ): ParsingResult[TransferOutCommonData] = {
    val v1.TransferOutCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      adminPartiesP,
      uuidP,
      mediatorIdP,
      protocolVersionP,
    ) = transferOutCommonDataP
    for {
      commonData <- ParsedDataV0V1.fromProto(
        saltP,
        sourceDomainP,
        mediatorIdP,
        stakeholdersP,
        adminPartiesP,
        uuidP,
      )
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(protocolVersionP)
      _ <- checkMediatorGroupForProtocolVersion(commonData, protocolVersion)
    } yield TransferOutCommonData(
      commonData.salt,
      commonData.sourceDomain,
      commonData.sourceMediator,
      commonData.stakeholders,
      commonData.adminParties,
      commonData.uuid,
    )(hashOps, SourceProtocolVersion(protocolVersion), Some(bytes))
  }

  final case class ParsedDataV0V1(
      salt: Salt,
      sourceDomain: SourceDomainId,
      sourceMediator: MediatorRef,
      stakeholders: Set[LfPartyId],
      adminParties: Set[LfPartyId],
      uuid: UUID,
  )
  private[this] object ParsedDataV0V1 {
    def fromProto(
        salt: Option[com.digitalasset.canton.crypto.v0.Salt],
        sourceDomain: String,
        mediatorRef: String,
        stakeholders: Seq[String],
        adminParties: Seq[String],
        uuid: String,
    ): ParsingResult[ParsedDataV0V1] =
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", salt)
        sourceDomain <- DomainId.fromProtoPrimitive(sourceDomain, "source_domain")
        sourceMediator <- MediatorRef.fromProtoPrimitive(mediatorRef, "source_mediator")
        stakeholders <- stakeholders.traverse(ProtoConverter.parseLfPartyId)
        adminParties <- adminParties.traverse(ProtoConverter.parseLfPartyId)
        uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuid)
      } yield ParsedDataV0V1(
        salt,
        SourceDomainId(sourceDomain),
        sourceMediator,
        stakeholders.toSet,
        adminParties.toSet,
        uuid,
      )
  }

}

/** Aggregates the data of a transfer-out request that is only sent to the involved participants
  */
sealed abstract class TransferOutView(hashOps: HashOps)
    extends MerkleTreeLeaf[TransferOutView](hashOps)
    with HasProtocolVersionedWrapper[TransferOutView]
    with ProtocolVersionedMemoizedEvidence {

  /** The salt used to blind the Merkle hash. */
  def salt: Salt

  def submitterMetadata: TransferSubmitterMetadata

  /** The id of the contract to be transferred. */
  def contractId: LfContractId

  /** The template of the contract to be transferred.
    * This is a dummy value until protocol version 4.
    */
  def templateId: LfTemplateId

  /** The domain to which the contract is transferred. */
  def targetDomain: TargetDomainId

  /** The sequenced event from the target domain
    * whose timestamp defines the baseline for measuring time periods on the target domain
    */
  def targetTimeProof: TimeProof

  def targetProtocolVersion: TargetProtocolVersion

  // TODO(#15159) Remove the note that it is defined iff...
  /** The [[com.digitalasset.canton.TransferCounter]] of the contract.
    * The value is defined iff the protocol versions is at least
    * [[com.digitalasset.canton.version.ProtocolVersion.CNTestNet]].
    */
  def transferCounterO: TransferCounterO

  val submitter: LfPartyId = submitterMetadata.submitter
  val submittingParticipant: LedgerParticipantId = submitterMetadata.submittingParticipant
  val applicationId: LedgerApplicationId = submitterMetadata.applicationId
  val submissionId: Option[LedgerSubmissionId] = submitterMetadata.submissionId
  val commandId: LedgerCommandId = submitterMetadata.commandId
  val workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  def hashPurpose: HashPurpose = HashPurpose.TransferOutView

  @transient override protected lazy val companionObj: TransferOutView.type = TransferOutView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  protected def toProtoV0: v0.TransferOutView

  protected def toProtoV1: v1.TransferOutView

  protected def toProtoV2: v2.TransferOutView
}

final case class TransferOutViewV0 private[data] (
    override val salt: Salt,
    submitterMetadata: TransferSubmitterMetadata,
    contractId: LfContractId,
    targetDomain: TargetDomainId,
    targetTimeProof: TimeProof,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutView.type],
    override val deserializedFrom: Option[ByteString],
) extends TransferOutView(hashOps) {

  override def templateId: LfTemplateId = TransferOutView.templateIdDefaultValue.defaultValue

  override def targetProtocolVersion: TargetProtocolVersion =
    TargetProtocolVersion(ProtocolVersion.v3)

  override def transferCounterO: TransferCounterO = None

  protected def toProtoV0: v0.TransferOutView =
    v0.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
    )

  protected def toProtoV1: v1.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V1 not supported."
  )

  protected def toProtoV2: v2.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V2 not supported."
  )

  override def pretty: Pretty[TransferOutViewV0] = prettyOfClass(
    param("submitterMetadata", _.submitterMetadata),
    param("contract id", _.contractId),
    param("template id", _.templateId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("target protocol version", _.targetProtocolVersion.v),
    param("salt", _.salt),
  )
}

final case class TransferOutViewV4 private[data] (
    override val salt: Salt,
    submitterMetadata: TransferSubmitterMetadata,
    contractId: LfContractId,
    targetDomain: TargetDomainId,
    targetTimeProof: TimeProof,
    targetProtocolVersion: TargetProtocolVersion,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransferOutView.type],
    override val deserializedFrom: Option[ByteString],
) extends TransferOutView(hashOps) {

  override def templateId: LfTemplateId = TransferOutView.templateIdDefaultValue.defaultValue

  override def transferCounterO: TransferCounterO = None

  protected def toProtoV0: v0.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V0 not supported."
  )

  protected def toProtoV1: v1.TransferOutView =
    v1.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      contractId = contractId.toProtoPrimitive,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
    )

  protected def toProtoV2: v2.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V2 not supported."
  )

  override def pretty: Pretty[TransferOutViewV4] = prettyOfClass(
    param("submitterMetadata", _.submitterMetadata),
    param("contract id", _.contractId),
    param("template id", _.templateId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("target protocol version", _.targetProtocolVersion.v),
    param("salt", _.salt),
  )
}

final case class TransferOutViewCNTestNet private[data] (
    salt: Salt,
    submitterMetadata: TransferSubmitterMetadata,
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
) extends TransferOutView(hashOps) {

  override def contractId: LfContractId = contract.contractId

  override def templateId: LfTemplateId =
    contract.rawContractInstance.contractInstance.unversioned.template

  override def transferCounterO: TransferCounterO = Some(transferCounter)

  protected def toProtoV0: v0.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V0 not supported."
  )

  protected def toProtoV1: v1.TransferOutView = throw new UnsupportedOperationException(
    "Serialization to V1 not supported."
  )

  protected def toProtoV2: v2.TransferOutView =
    v2.TransferOutView(
      salt = Some(salt.toProtoV0),
      submitter = submitter,
      targetDomain = targetDomain.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV0),
      targetProtocolVersion = targetProtocolVersion.v.toProtoPrimitive,
      submittingParticipant = submittingParticipant,
      applicationId = applicationId,
      submissionId = submissionId.getOrElse(""),
      workflowId = workflowId.getOrElse(""),
      commandId = commandId,
      transferCounter = transferCounter.toProtoPrimitive,
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      contract = Some(contract.toProtoV1),
    )

  override def pretty: Pretty[TransferOutViewCNTestNet] = prettyOfClass(
    param("submitterMetadata", _.submitterMetadata),
    param("contract id", _.contractId),
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

  private[TransferOutView] final case class ParsedDataV0V1(
      salt: Salt,
      submitter: LfPartyId,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetDomainPV: TargetProtocolVersion,
      targetTimeProof: TimeProof,
  )
  private[TransferOutView] object ParsedDataV0V1 {
    def fromProto(
        hashOps: HashOps,
        saltP: Option[com.digitalasset.canton.crypto.v0.Salt],
        submitterP: String,
        contractIdP: String,
        targetDomainP: String,
        targetTimeProofP: Option[com.digitalasset.canton.time.v0.TimeProof],
        targetProtocolVersion: ProtocolVersion,
    ): ParsingResult[ParsedDataV0V1] = {
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
        submitter <- ProtoConverter.parseLfPartyId(submitterP)
        contractId <- ProtoConverter.parseLfContractId(contractIdP)
        targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")

        targetTimeProof <- ProtoConverter
          .required("targetTimeProof", targetTimeProofP)
          .flatMap(TimeProof.fromProtoV0(targetProtocolVersion, hashOps))
      } yield ParsedDataV0V1(
        salt,
        submitter,
        contractId,
        TargetDomainId(targetDomain),
        TargetProtocolVersion(targetProtocolVersion),
        targetTimeProof,
      )
    }
  }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.v4)(v1.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(ProtocolVersion.CNTestNet)(v2.TransferOutView)(
      supportedProtoVersionMemoized(_)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  private lazy val rpv4: RepresentativeProtocolVersion[TransferOutView.type] =
    protocolVersionRepresentativeFor(ProtocolVersion.v4)

  private lazy val rpvMultidomain: RepresentativeProtocolVersion[TransferOutView.type] =
    protocolVersionRepresentativeFor(ProtocolVersion.CNTestNet)

  lazy val submittingParticipantDefaultValue: DefaultValueUntilExclusive[LedgerParticipantId] =
    DefaultValueUntilExclusive(
      _.submitterMetadata.submittingParticipant,
      "submitterMetadata.submittingParticipant",
      rpvMultidomain,
      LedgerParticipantId.assertFromString("no-participant-id"),
    )

  lazy val commandIdDefaultValue: DefaultValueUntilExclusive[LedgerCommandId] =
    DefaultValueUntilExclusive(
      _.submitterMetadata.commandId,
      "submitterMetadata.commandId",
      rpvMultidomain,
      LedgerCommandId.assertFromString("no-command-id"),
    )

  lazy val applicationIdDefaultValue: DefaultValueUntilExclusive[LedgerApplicationId] =
    DefaultValueUntilExclusive(
      _.submitterMetadata.applicationId,
      "submitterMetadata.applicationId",
      rpvMultidomain,
      LedgerApplicationId.assertFromString("no-application-id"),
    )

  lazy val submissionIdDefaultValue: DefaultValueUntilExclusive[Option[LedgerSubmissionId]] =
    DefaultValueUntilExclusive(
      _.submitterMetadata.submissionId,
      "submitterMetadata.submissionId",
      rpvMultidomain,
      None,
    )

  lazy val workflowIdDefaultValue: DefaultValueUntilExclusive[Option[LfWorkflowId]] =
    DefaultValueUntilExclusive(
      _.submitterMetadata.workflowId,
      "submitterMetadata.worfklowId",
      rpvMultidomain,
      None,
    )

  lazy val templateIdDefaultValue: DefaultValueUntilExclusive[LfTemplateId] =
    DefaultValueUntilExclusive(
      _.templateId,
      "templateId",
      rpvMultidomain,
      LfTemplateId.assertFromString("no-package-id:no.module.name:no.entity.name"),
    )

  lazy val targetProtocolVersionDefaultValue: DefaultValueUntilExclusive[TargetProtocolVersion] =
    DefaultValueUntilExclusive(
      _.targetProtocolVersion,
      "targetProtocolVersion",
      rpv4,
      TargetProtocolVersion(ProtocolVersion.v3),
    )

  def create(hashOps: HashOps)(
      salt: Salt,
      submitterMetadata: TransferSubmitterMetadata,
      creatingTransactionId: TransactionId,
      contract: SerializableContract,
      targetDomain: TargetDomainId,
      targetTimeProof: TimeProof,
      sourceProtocolVersion: SourceProtocolVersion,
      targetProtocolVersion: TargetProtocolVersion,
      transferCounter: TransferCounterO,
  ): TransferOutView =
    if (sourceProtocolVersion.v < ProtocolVersion.v4)
      TransferOutViewV0(
        salt,
        submitterMetadata,
        contract.contractId,
        targetDomain,
        targetTimeProof,
      )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.v), None)
    else if (sourceProtocolVersion.v < ProtocolVersion.CNTestNet)
      TransferOutViewV4(
        salt,
        submitterMetadata,
        contract.contractId,
        targetDomain,
        targetTimeProof,
        targetProtocolVersion,
      )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.v), None)
    else
      TransferOutViewCNTestNet(
        salt,
        submitterMetadata,
        creatingTransactionId,
        contract,
        targetDomain,
        targetTimeProof,
        targetProtocolVersion,
        transferCounter.getOrElse(throw new IllegalArgumentException("Missing transfer counter.")),
      )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.v), None)

  private[this] def fromProtoV0(hashOps: HashOps, transferOutViewP: v0.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutViewV0] = {
    val v0.TransferOutView(saltP, submitterP, contractIdP, targetDomainP, targetTimeProofP) =
      transferOutViewP
    for {
      commonData <- ParsedDataV0V1.fromProto(
        hashOps,
        saltP,
        submitterP,
        contractIdP,
        targetDomainP,
        targetTimeProofP,
        ProtocolVersion.v3,
      )
    } yield TransferOutViewV0(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        applicationIdDefaultValue.defaultValue,
        submittingParticipantDefaultValue.defaultValue,
        commandIdDefaultValue.defaultValue,
        submissionId = submissionIdDefaultValue.defaultValue,
        workflowId = workflowIdDefaultValue.defaultValue,
      ),
      commonData.contractId,
      commonData.targetDomain,
      commonData.targetTimeProof,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
  }

  private[this] def fromProtoV1(hashOps: HashOps, transferOutViewP: v1.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutViewV4] = {
    val v1.TransferOutView(
      saltP,
      submitterP,
      contractIdP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
    ) = transferOutViewP

    for {
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP)
      commonData <- ParsedDataV0V1.fromProto(
        hashOps,
        saltP,
        submitterP,
        contractIdP,
        targetDomainP,
        targetTimeProofP,
        protocolVersion,
      )
    } yield TransferOutViewV4(
      commonData.salt,
      TransferSubmitterMetadata(
        commonData.submitter,
        applicationIdDefaultValue.defaultValue,
        submittingParticipantDefaultValue.defaultValue,
        commandIdDefaultValue.defaultValue,
        submissionId = submissionIdDefaultValue.defaultValue,
        workflowId = workflowIdDefaultValue.defaultValue,
      ),
      commonData.contractId,
      commonData.targetDomain,
      commonData.targetTimeProof,
      commonData.targetDomainPV,
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(1)),
      Some(bytes),
    )
  }

  private[this] def fromProtoV2(hashOps: HashOps, transferOutViewP: v2.TransferOutView)(
      bytes: ByteString
  ): ParsingResult[TransferOutViewCNTestNet] = {
    val v2.TransferOutView(
      saltP,
      submitterP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
      submittingParticipantP,
      applicationIdP,
      submissionIdP,
      workflowIdP,
      commandIdP,
      transferCounter,
      creatingTransactionIdP,
      contractPO,
    ) = transferOutViewP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV0, "salt", saltP)
      submitter <- ProtoConverter.parseLfPartyId(submitterP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")
      targetProtocolVersion <- ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP)
      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV0(targetProtocolVersion, hashOps))
      submittingParticipant <- ProtoConverter.parseLfParticipantId(submittingParticipantP)
      applicationId <- ProtoConverter.parseLFApplicationId(applicationIdP)
      submissionId <- ProtoConverter.parseLFSubmissionIdO(submissionIdP)
      workflowId <- ProtoConverter.parseLFWorkflowIdO(workflowIdP)
      commandId <- ProtoConverter.parseCommandId(commandIdP)
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      contract <- ProtoConverter
        .required("TransferOutViewTree.contract", contractPO)
        .flatMap(SerializableContract.fromProtoV1)

    } yield TransferOutViewCNTestNet(
      salt,
      TransferSubmitterMetadata(
        submitter,
        applicationId,
        submittingParticipant,
        commandId,
        submissionId,
        workflowId,
      ),
      creatingTransactionId,
      contract,
      TargetDomainId(targetDomain),
      targetTimeProof,
      TargetProtocolVersion(targetProtocolVersion),
      TransferCounter(transferCounter),
    )(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(2)),
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

  def submitter: LfPartyId = view.submitter

  def submitterMetadata: TransferSubmitterMetadata = view.submitterMetadata
  def workflowId: Option[LfWorkflowId] = view.workflowId

  def stakeholders: Set[LfPartyId] = commonData.stakeholders

  def adminParties: Set[LfPartyId] = commonData.adminParties

  def contractId: LfContractId = view.contractId

  def templateId: LfTemplateId = view.templateId

  def transferCounter: TransferCounterO = view.transferCounterO

  def sourceDomain: SourceDomainId = commonData.sourceDomain

  def targetDomain: TargetDomainId = view.targetDomain

  def targetDomainPV: TargetProtocolVersion = view.targetProtocolVersion

  def targetTimeProof: TimeProof = view.targetTimeProof

  def mediatorMessage: TransferOutMediatorMessage = tree.mediatorMessage

  override def domainId: DomainId = sourceDomain.unwrap

  override def mediator: MediatorRef = commonData.sourceMediator

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
      tree <- {
        // No validation because of wrongly serialized GenTransferViewTree (used protoV0 for pv <= 5 in the past);
        // thus resulting in data dumps that fail deserialization validation (which expects protoV0 for pv <= 3, protoV1
        // otherwise).
        if (sourceProtocolVersion.v <= ProtocolVersion.v5) {
          TransferOutViewTree.fromByteStringUnsafe((crypto, sourceProtocolVersion.v))(bytes)
        } else {
          TransferOutViewTree.fromByteString(crypto, sourceProtocolVersion.v)(bytes)
        }
      }
      _ <- EitherUtil.condUnitE(
        tree.isFullyUnblinded,
        OtherError(s"Transfer-out request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullTransferOutTree(tree)
}
