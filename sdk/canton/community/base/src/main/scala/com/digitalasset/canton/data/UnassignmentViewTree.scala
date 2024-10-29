// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.UnassignmentMediatorMessage
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, TimeProof}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{LfPartyId, LfWorkflowId, ReassignmentCounter}
import com.google.protobuf.ByteString

import java.util.UUID

/** An unassignment request embedded in a Merkle tree. The view may or may not be blinded. */
final case class UnassignmentViewTree(
    commonData: MerkleTreeLeaf[UnassignmentCommonData],
    view: MerkleTree[UnassignmentView],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      UnassignmentViewTree.type
    ],
    hashOps: HashOps,
) extends GenReassignmentViewTree[
      UnassignmentCommonData,
      UnassignmentView,
      UnassignmentViewTree,
      UnassignmentMediatorMessage,
    ](commonData, view)(hashOps)
    with HasProtocolVersionedWrapper[UnassignmentViewTree] {

  def submittingParticipant: ParticipantId =
    commonData.tryUnwrap.submitterMetadata.submittingParticipant

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[UnassignmentViewTree] = {

    if (
      optimizedBlindingPolicy.applyOrElse(
        commonData.rootHash,
        (_: RootHash) => RevealSubtree,
      ) != RevealSubtree
    )
      throw new IllegalArgumentException("Blinding of common data is not supported.")

    UnassignmentViewTree(
      commonData,
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)
  }

  protected[this] override def createMediatorMessage(
      blindedTree: UnassignmentViewTree,
      submittingParticipantSignature: Signature,
  ): UnassignmentMediatorMessage =
    UnassignmentMediatorMessage(blindedTree, submittingParticipantSignature)

  override protected def pretty: Pretty[UnassignmentViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: UnassignmentViewTree.type =
    UnassignmentViewTree
}

object UnassignmentViewTree
    extends HasProtocolVersionedWithContextAndValidationWithSourceProtocolVersionCompanion[
      UnassignmentViewTree,
      HashOps,
    ] {

  override val name: String = "UnassignmentViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.ReassignmentViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      commonData: MerkleTreeLeaf[UnassignmentCommonData],
      view: MerkleTree[UnassignmentView],
      sourceProtocolVersion: Source[ProtocolVersion],
      hashOps: HashOps,
  ): UnassignmentViewTree =
    UnassignmentViewTree(commonData, view)(
      UnassignmentViewTree.protocolVersionRepresentativeFor(sourceProtocolVersion.unwrap),
      hashOps,
    )

  def fromProtoV30(context: (HashOps, Source[ProtocolVersion]))(
      unassignmentViewTreeP: v30.ReassignmentViewTree
  ): ParsingResult[UnassignmentViewTree] = {
    val (hashOps, expectedProtocolVersion) = context

    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      res <- GenReassignmentViewTree.fromProtoV30(
        UnassignmentCommonData.fromByteString(expectedProtocolVersion.unwrap)(
          (hashOps, expectedProtocolVersion)
        ),
        UnassignmentView.fromByteString(expectedProtocolVersion.unwrap)(hashOps),
      )((commonData, view) =>
        UnassignmentViewTree(commonData, view)(
          rpv,
          hashOps,
        )
      )(unassignmentViewTreeP)
    } yield res

  }
}

/** Aggregates the data of an unassignment request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param sourceDomain The domain to which the unassignment request is sent
  * @param sourceMediatorGroup The mediator that coordinates the unassignment request on the source domain
  * @param stakeholders Information about the stakeholders and signatories
  * @param confirmingReassigningParticipants The list of confirming reassigning participants
  * @param uuid The request UUID of the unassignment
  * @param submitterMetadata information about the submission
  */
final case class UnassignmentCommonData private (
    override val salt: Salt,
    sourceDomain: Source[DomainId],
    sourceMediatorGroup: MediatorGroupRecipient,
    stakeholders: Stakeholders,
    confirmingReassigningParticipants: Set[ParticipantId],
    uuid: UUID,
    submitterMetadata: ReassignmentSubmitterMetadata,
)(
    hashOps: HashOps,
    val sourceProtocolVersion: Source[ProtocolVersion],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[UnassignmentCommonData](hashOps)
    with HasProtocolVersionedWrapper[UnassignmentCommonData]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: UnassignmentCommonData.type =
    UnassignmentCommonData

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[UnassignmentCommonData.type] =
    UnassignmentCommonData.protocolVersionRepresentativeFor(sourceProtocolVersion.unwrap)

  protected def toProtoV30: v30.UnassignmentCommonData =
    v30.UnassignmentCommonData(
      salt = Some(salt.toProtoV30),
      sourceDomain = sourceDomain.unwrap.toProtoPrimitive,
      sourceMediatorGroup = sourceMediatorGroup.group.value,
      stakeholders = Some(stakeholders.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
      confirmingReassigningParticipantUids =
        confirmingReassigningParticipants.toSeq.map(_.uid.toProtoPrimitive),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.UnassignmentCommonData

  def confirmingParties: Map[LfPartyId, PositiveInt] =
    stakeholders.all.map(_ -> PositiveInt.one).toMap

  override protected def pretty: Pretty[UnassignmentCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("source domain", _.sourceDomain),
    param("source mediator group", _.sourceMediatorGroup),
    param("stakeholders", _.stakeholders),
    param("confirming reassigning participants", _.confirmingReassigningParticipants),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object UnassignmentCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      UnassignmentCommonData,
      (HashOps, Source[ProtocolVersion]),
    ] {
  override val name: String = "UnassignmentCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.UnassignmentCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceDomain: Source[DomainId],
      sourceMediatorGroup: MediatorGroupRecipient,
      stakeholders: Stakeholders,
      confirmingReassigningParticipants: Set[ParticipantId],
      uuid: UUID,
      submitterMetadata: ReassignmentSubmitterMetadata,
      sourceProtocolVersion: Source[ProtocolVersion],
  ): UnassignmentCommonData = UnassignmentCommonData(
    salt = salt,
    sourceDomain = sourceDomain,
    sourceMediatorGroup = sourceMediatorGroup,
    stakeholders = stakeholders,
    confirmingReassigningParticipants = confirmingReassigningParticipants,
    uuid = uuid,
    submitterMetadata = submitterMetadata,
  )(hashOps, sourceProtocolVersion, None)

  private[this] def fromProtoV30(
      context: (HashOps, Source[ProtocolVersion]),
      unassignmentCommonDataP: v30.UnassignmentCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[UnassignmentCommonData] = {
    val (hashOps, sourceProtocolVersion) = context
    val v30.UnassignmentCommonData(
      saltP,
      sourceDomainP,
      stakeholdersP,
      confirmingReassigningParticipantUidsP,
      uuidP,
      sourceMediatorGroupP,
      submitterMetadataPO,
    ) = unassignmentCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      sourceDomain <- DomainId.fromProtoPrimitive(sourceDomainP, "source_domain").map(Source(_))
      sourceMediatorGroup <- ProtoConverter.parseNonNegativeInt(
        "source_mediator_group",
        sourceMediatorGroupP,
      )

      stakeholders <- ProtoConverter.parseRequired(
        Stakeholders.fromProtoV30,
        "stakeholders",
        stakeholdersP,
      )
      confirmingReassigningParticipants <- confirmingReassigningParticipantUidsP.traverse(uid =>
        UniqueIdentifier
          .fromProtoPrimitive(uid, "confirming_reassigning_participant_uids")
          .map(ParticipantId(_))
      )
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      submitterMetadata <- ProtoConverter
        .required("submitter_metadata", submitterMetadataPO)
        .flatMap(ReassignmentSubmitterMetadata.fromProtoV30)

    } yield UnassignmentCommonData(
      salt,
      sourceDomain,
      MediatorGroupRecipient(sourceMediatorGroup),
      stakeholders = stakeholders,
      confirmingReassigningParticipants.toSet,
      uuid,
      submitterMetadata,
    )(hashOps, sourceProtocolVersion, Some(bytes))
  }
}

/** Aggregates the data of an unassignment request that is only sent to the involved participants
  */
/** @param salt The salt used to blind the Merkle hash.
  * @param contract Contract being reassigned
  * @param creatingTransactionId Id of the transaction that created the contract
  * @param targetDomain The domain to which the contract is reassigned.
  * @param targetTimeProof The sequenced event from the target domain whose timestamp defines
  *                        the baseline for measuring time periods on the target domain
  * @param targetProtocolVersion Protocol version of the target domain
  */
final case class UnassignmentView private (
    override val salt: Salt,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    targetDomain: Target[DomainId],
    targetTimeProof: TimeProof,
    targetProtocolVersion: Target[ProtocolVersion],
    reassignmentCounter: ReassignmentCounter,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      UnassignmentView.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[UnassignmentView](hashOps)
    with HasProtocolVersionedWrapper[UnassignmentView]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: UnassignmentView.type = UnassignmentView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  def hashPurpose: HashPurpose = HashPurpose.UnassignmentView

  def templateId: LfTemplateId =
    contract.rawContractInstance.contractInstance.unversioned.template

  protected def toProtoV30: v30.UnassignmentView =
    v30.UnassignmentView(
      salt = Some(salt.toProtoV30),
      targetDomain = targetDomain.unwrap.toProtoPrimitive,
      targetTimeProof = Some(targetTimeProof.toProtoV30),
      targetProtocolVersion = targetProtocolVersion.unwrap.toProtoPrimitive,
      reassignmentCounter = reassignmentCounter.toProtoPrimitive,
      creatingTransactionId = creatingTransactionId.toProtoPrimitive,
      contract = Some(contract.toProtoV30),
    )

  override protected def pretty: Pretty[UnassignmentView] = prettyOfClass(
    param("creating transaction id", _.creatingTransactionId),
    param("template id", _.templateId),
    param("target domain", _.targetDomain),
    param("target time proof", _.targetTimeProof),
    param("target protocol version", _.targetProtocolVersion),
    param("reassignment counter", _.reassignmentCounter),
    param(
      "contract id",
      _.contract.contractId,
    ), // do not log contract details because it contains confidential data
    param("salt", _.salt),
  )
}

object UnassignmentView
    extends HasMemoizedProtocolVersionedWithContextCompanion[UnassignmentView, HashOps] {
  override val name: String = "UnassignmentView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.UnassignmentView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      contract: SerializableContract,
      creatingTransactionId: TransactionId,
      targetDomain: Target[DomainId],
      targetTimeProof: TimeProof,
      sourceProtocolVersion: Source[ProtocolVersion],
      targetProtocolVersion: Target[ProtocolVersion],
      reassignmentCounter: ReassignmentCounter,
  ): UnassignmentView =
    UnassignmentView(
      salt,
      contract,
      creatingTransactionId,
      targetDomain,
      targetTimeProof,
      targetProtocolVersion,
      reassignmentCounter,
    )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.unwrap), None)

  private[this] def fromProtoV30(hashOps: HashOps, unassignmentViewP: v30.UnassignmentView)(
      bytes: ByteString
  ): ParsingResult[UnassignmentView] = {
    val v30.UnassignmentView(
      saltP,
      targetDomainP,
      targetTimeProofP,
      targetProtocolVersionP,
      reassignmentCounterP,
      creatingTransactionIdP,
      contractPO,
    ) = unassignmentViewP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "targetDomain")
      targetProtocolVersion <- ProtocolVersion.fromProtoPrimitive(targetProtocolVersionP)
      targetTimeProof <- ProtoConverter
        .required("targetTimeProof", targetTimeProofP)
        .flatMap(TimeProof.fromProtoV30(targetProtocolVersion, hashOps))
      creatingTransactionId <- TransactionId.fromProtoPrimitive(creatingTransactionIdP)
      contract <- ProtoConverter
        .required("UnassignmentViewTree.contract", contractPO)
        .flatMap(SerializableContract.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield UnassignmentView(
      salt,
      contract,
      creatingTransactionId,
      Target(targetDomain),
      targetTimeProof,
      Target(targetProtocolVersion),
      ReassignmentCounter(reassignmentCounterP),
    )(
      hashOps,
      rpv,
      Some(bytes),
    )

  }
}

/** A fully unblinded [[UnassignmentViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullUnassignmentTree(tree: UnassignmentViewTree)
    extends ReassignmentViewTree
    with HasToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "An unassignment request must be fully unblinded")

  private[this] val commonData = tree.commonData.tryUnwrap
  private[this] val view = tree.view.tryUnwrap

  // Submissions
  def submitterMetadata: ReassignmentSubmitterMetadata = commonData.submitterMetadata
  def submitter: LfPartyId = submitterMetadata.submitter
  def workflowId: Option[LfWorkflowId] = submitterMetadata.workflowId

  // Parties and participants
  // TODO(#21072) Check informees and stakeholders are compatible
  override def informees: Set[LfPartyId] = view.contract.metadata.stakeholders
  def stakeholders: Stakeholders = commonData.stakeholders
  override def confirmingReassigningParticipants: Set[ParticipantId] =
    commonData.confirmingReassigningParticipants

  // Contract
  def contract: SerializableContract = view.contract
  def contractId: LfContractId = view.contract.contractId
  def templateId: LfTemplateId = view.templateId
  def reassignmentCounter: ReassignmentCounter = view.reassignmentCounter

  // Domains
  override def domainId: DomainId = sourceDomain.unwrap
  override def sourceDomain: Source[DomainId] = commonData.sourceDomain
  override def targetDomain: Target[DomainId] = view.targetDomain
  def targetTimeProof: TimeProof = view.targetTimeProof
  def targetProtocolVersion: Target[ProtocolVersion] = view.targetProtocolVersion

  def mediatorMessage(
      submittingParticipantSignature: Signature
  ): UnassignmentMediatorMessage = tree.mediatorMessage(submittingParticipantSignature)

  override def mediator: MediatorGroupRecipient = commonData.sourceMediatorGroup

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override protected def pretty: Pretty[FullUnassignmentTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString: ByteString = tree.toByteString
}

object FullUnassignmentTree {
  def fromByteString(
      crypto: CryptoPureApi,
      sourceProtocolVersion: Source[ProtocolVersion],
  )(bytes: ByteString): ParsingResult[FullUnassignmentTree] =
    for {
      tree <- UnassignmentViewTree.fromByteString(crypto, sourceProtocolVersion)(bytes)
      _ <- Either.cond(
        tree.isFullyUnblinded,
        (),
        OtherError(s"Unassignment request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullUnassignmentTree(tree)
}
