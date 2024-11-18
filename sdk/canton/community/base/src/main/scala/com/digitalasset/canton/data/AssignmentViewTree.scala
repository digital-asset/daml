// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{
  AssignmentMediatorMessage,
  DeliveredUnassignmentResult,
}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  NoOpeningErrors,
  SequencedEvent,
  SignedContent,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, ReassignmentCounter}
import com.google.protobuf.ByteString

import java.util.UUID

/** an assignment request embedded in a Merkle tree. The view may or may not be blinded. */
final case class AssignmentViewTree(
    commonData: MerkleTreeLeaf[AssignmentCommonData],
    view: MerkleTree[AssignmentView],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AssignmentViewTree.type
    ],
    hashOps: HashOps,
) extends GenReassignmentViewTree[
      AssignmentCommonData,
      AssignmentView,
      AssignmentViewTree,
      AssignmentMediatorMessage,
    ](commonData, view)(hashOps)
    with HasProtocolVersionedWrapper[AssignmentViewTree]
    with ReassignmentViewTree {

  override private[data] def withBlindedSubtrees(
      optimizedBlindingPolicy: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[AssignmentViewTree] = {

    if (
      optimizedBlindingPolicy.applyOrElse(
        commonData.rootHash,
        (_: RootHash) => RevealSubtree,
      ) != RevealSubtree
    )
      throw new IllegalArgumentException("Blinding of common data is not supported.")

    AssignmentViewTree(
      commonData,
      view.doBlind(optimizedBlindingPolicy),
    )(representativeProtocolVersion, hashOps)
  }

  protected[this] override def createMediatorMessage(
      blindedTree: AssignmentViewTree,
      submittingParticipantSignature: Signature,
  ): AssignmentMediatorMessage =
    AssignmentMediatorMessage(blindedTree, submittingParticipantSignature)

  override protected def pretty: Pretty[AssignmentViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: AssignmentViewTree.type =
    AssignmentViewTree
}

object AssignmentViewTree
    extends HasProtocolVersionedWithContextAndValidationWithTargetProtocolVersionCompanion[
      AssignmentViewTree,
      HashOps,
    ] {

  override val name: String = "AssignmentViewTree"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.ReassignmentViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def apply(
      commonData: MerkleTreeLeaf[AssignmentCommonData],
      view: MerkleTree[AssignmentView],
      targetProtocolVersion: Target[ProtocolVersion],
      hashOps: HashOps,
  ): AssignmentViewTree =
    AssignmentViewTree(commonData, view)(
      AssignmentViewTree.protocolVersionRepresentativeFor(targetProtocolVersion.unwrap),
      hashOps,
    )

  def fromProtoV30(context: (HashOps, Target[ProtocolVersion]))(
      assignmentViewTreeP: v30.ReassignmentViewTree
  ): ParsingResult[AssignmentViewTree] = {
    val (hashOps, expectedProtocolVersion) = context
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      res <- GenReassignmentViewTree.fromProtoV30(
        AssignmentCommonData.fromByteString(expectedProtocolVersion.unwrap)(
          (hashOps, expectedProtocolVersion)
        ),
        AssignmentView.fromByteString(expectedProtocolVersion.unwrap)(hashOps),
      )((commonData, view) =>
        AssignmentViewTree(commonData, view)(
          rpv,
          hashOps,
        )
      )(assignmentViewTreeP)
    } yield res
  }
}

/** Aggregates the data of an assignment request that is sent to the mediator and the involved participants.
  *
  * @param salt Salt for blinding the Merkle hash
  * @param targetDomain The domain on which the contract is assigned
  * @param targetMediatorGroup The mediator that coordinates the assignment request on the target domain
  * @param stakeholders The stakeholders of the reassigned contract
  * @param uuid The uuid of the assignment request
  * @param submitterMetadata information about the submission
  * @param reassigningParticipants The list of reassigning participants
  */
final case class AssignmentCommonData private (
    override val salt: Salt,
    targetDomain: Target[DomainId],
    targetMediatorGroup: MediatorGroupRecipient,
    stakeholders: Stakeholders,
    uuid: UUID,
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassigningParticipants: ReassigningParticipants,
)(
    hashOps: HashOps,
    val targetProtocolVersion: Target[ProtocolVersion],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[AssignmentCommonData](hashOps)
    with HasProtocolVersionedWrapper[AssignmentCommonData]
    with ReassignmentCommonData {

  @transient override protected lazy val companionObj: AssignmentCommonData.type =
    AssignmentCommonData

  override val representativeProtocolVersion
      : RepresentativeProtocolVersion[AssignmentCommonData.type] =
    AssignmentCommonData.protocolVersionRepresentativeFor(targetProtocolVersion.unwrap)

  protected def toProtoV30: v30.AssignmentCommonData =
    v30.AssignmentCommonData(
      salt = Some(salt.toProtoV30),
      targetDomain = targetDomain.unwrap.toProtoPrimitive,
      targetMediatorGroup = targetMediatorGroup.group.value,
      stakeholders = Some(stakeholders.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
      confirmingReassigningParticipantUids =
        reassigningParticipants.confirming.map(_.uid.toProtoPrimitive).toSeq,
      observingReassigningParticipantUids =
        reassigningParticipants.observing.map(_.uid.toProtoPrimitive).toSeq,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.AssignmentCommonData

  override protected def pretty: Pretty[AssignmentCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("target domain", _.targetDomain),
    param("target mediator group", _.targetMediatorGroup),
    param("stakeholders", _.stakeholders),
    param("reassigning participants", _.reassigningParticipants),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object AssignmentCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      AssignmentCommonData,
      (HashOps, Target[ProtocolVersion]),
    ] {
  override val name: String = "AssignmentCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.AssignmentCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetDomain: Target[DomainId],
      targetMediatorGroup: MediatorGroupRecipient,
      stakeholders: Stakeholders,
      uuid: UUID,
      submitterMetadata: ReassignmentSubmitterMetadata,
      targetProtocolVersion: Target[ProtocolVersion],
      reassigningParticipants: ReassigningParticipants,
  ): AssignmentCommonData = AssignmentCommonData(
    salt = salt,
    targetDomain = targetDomain,
    targetMediatorGroup = targetMediatorGroup,
    stakeholders = stakeholders,
    uuid = uuid,
    submitterMetadata = submitterMetadata,
    reassigningParticipants = reassigningParticipants,
  )(hashOps, targetProtocolVersion, None)

  private[this] def fromProtoV30(
      context: (HashOps, Target[ProtocolVersion]),
      assignmentCommonDataP: v30.AssignmentCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[AssignmentCommonData] = {
    val (hashOps, targetProtocolVersion) = context
    val v30.AssignmentCommonData(
      saltP,
      targetDomainP,
      stakeholdersP,
      uuidP,
      targetMediatorGroupP,
      submitterMetadataPO,
      confirmingReassigningParticipantUidsP,
      observingReassigningParticipantUidsP,
    ) = assignmentCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetDomain <- DomainId.fromProtoPrimitive(targetDomainP, "target_domain").map(Target(_))
      targetMediatorGroup <- ProtoConverter.parseNonNegativeInt(
        "target_mediator_group",
        targetMediatorGroupP,
      )
      stakeholders <- ProtoConverter.parseRequired(
        Stakeholders.fromProtoV30,
        "stakeholders",
        stakeholdersP,
      )
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      submitterMetadata <- ProtoConverter
        .required("submitter_metadata", submitterMetadataPO)
        .flatMap(ReassignmentSubmitterMetadata.fromProtoV30)

      confirmingReassigningParticipants <- confirmingReassigningParticipantUidsP.traverse(uid =>
        UniqueIdentifier
          .fromProtoPrimitive(uid, "confirming_reassigning_participant_uids")
          .map(ParticipantId(_))
      )
      observingReassigningParticipants <- observingReassigningParticipantUidsP.traverse(uid =>
        UniqueIdentifier
          .fromProtoPrimitive(uid, "observing_reassigning_participant_uids")
          .map(ParticipantId(_))
      )

      reassigningParticipants <- ReassigningParticipants
        .create(
          confirming = confirmingReassigningParticipants.toSet,
          observing = observingReassigningParticipants.toSet,
        )
        .leftMap(ProtoDeserializationError.InvariantViolation(field = "confirming", _))

    } yield AssignmentCommonData(
      salt,
      targetDomain,
      MediatorGroupRecipient(targetMediatorGroup),
      stakeholders = stakeholders,
      uuid,
      submitterMetadata,
      reassigningParticipants = reassigningParticipants,
    )(hashOps, targetProtocolVersion, Some(bytes))
  }
}

/** Aggregates the data of an assignment request that is only sent to the involved participants
  *
  * @param salt                    The salt to blind the Merkle hash
  * @param contract                The contract to be reassigned including the instance
  * @param unassignmentResultEvent The signed deliver event of the unassignment result message
  * @param sourceProtocolVersion   Protocol version of the source domain.
  * @param reassignmentCounter     The [[com.digitalasset.canton.ReassignmentCounter]] of the contract.
  */
final case class AssignmentView private (
    override val salt: Salt,
    contract: SerializableContract,
    unassignmentResultEvent: DeliveredUnassignmentResult,
    sourceProtocolVersion: Source[ProtocolVersion],
    reassignmentCounter: ReassignmentCounter,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[AssignmentView.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[AssignmentView](hashOps)
    with HasProtocolVersionedWrapper[AssignmentView]
    with ReassignmentView {

  @transient override protected lazy val companionObj: AssignmentView.type = AssignmentView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  def hashPurpose: HashPurpose = HashPurpose.AssignmentView

  protected def toProtoV30: v30.AssignmentView =
    v30.AssignmentView(
      salt = Some(salt.toProtoV30),
      contract = Some(contract.toProtoV30),
      unassignmentResultEvent = unassignmentResultEvent.result.toByteString,
      sourceProtocolVersion = sourceProtocolVersion.unwrap.toProtoPrimitive,
      reassignmentCounter = reassignmentCounter.toProtoPrimitive,
    )

  override protected def pretty: Pretty[AssignmentView] = prettyOfClass(
    param("unassignment result event", _.unassignmentResultEvent),
    param("source protocol version", _.sourceProtocolVersion),
    param("reassignment counter", _.reassignmentCounter),
    param(
      "contract id",
      _.contract.contractId,
    ), // do not log contract details because it contains confidential data
    param("salt", _.salt),
  )
}

object AssignmentView
    extends HasMemoizedProtocolVersionedWithContextCompanion[AssignmentView, HashOps] {
  override val name: String = "AssignmentView"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v32)(v30.AssignmentView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      contract: SerializableContract,
      unassignmentResultEvent: DeliveredUnassignmentResult,
      sourceProtocolVersion: Source[ProtocolVersion],
      targetProtocolVersion: Target[ProtocolVersion],
      reassignmentCounter: ReassignmentCounter,
  ): Either[String, AssignmentView] = Either
    .catchOnly[IllegalArgumentException](
      AssignmentView(
        salt,
        contract,
        unassignmentResultEvent,
        sourceProtocolVersion,
        reassignmentCounter,
      )(hashOps, protocolVersionRepresentativeFor(targetProtocolVersion.unwrap), None)
    )
    .leftMap(_.getMessage)

  private[this] def fromProtoV30(hashOps: HashOps, assignmentViewP: v30.AssignmentView)(
      bytes: ByteString
  ): ParsingResult[AssignmentView] = {
    val v30.AssignmentView(
      saltP,
      contractP,
      unassignmentResultEventP,
      sourceProtocolVersionP,
      reassignmentCounterP,
    ) =
      assignmentViewP
    for {
      protocolVersion <- ProtocolVersion.fromProtoPrimitive(sourceProtocolVersionP)
      sourceProtocolVersion = Source(protocolVersion)
      commonData <- CommonData.fromProto(
        hashOps,
        saltP,
        unassignmentResultEventP,
        sourceProtocolVersion,
      )
      contract <- ProtoConverter
        .required("contract", contractP)
        .flatMap(SerializableContract.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield AssignmentView(
      commonData.salt,
      contract,
      commonData.unassignmentResultEvent,
      commonData.sourceProtocolVersion,
      ReassignmentCounter(reassignmentCounterP),
    )(hashOps, rpv, Some(bytes))
  }

  private[AssignmentView] final case class CommonData(
      salt: Salt,
      unassignmentResultEvent: DeliveredUnassignmentResult,
      sourceProtocolVersion: Source[ProtocolVersion],
  )

  private[AssignmentView] object CommonData {
    def fromProto(
        hashOps: HashOps,
        saltP: Option[com.digitalasset.canton.crypto.v30.Salt],
        unassignmentResultEventP: ByteString,
        sourceProtocolVersion: Source[ProtocolVersion],
    ): ParsingResult[CommonData] =
      for {
        salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
        // UnassignmentResultEvent deserialization
        unassignmentResultEventMC <- SignedContent
          .fromByteString(sourceProtocolVersion.unwrap)(unassignmentResultEventP)
          .flatMap(
            _.deserializeContent(
              SequencedEvent.fromByteStringOpen(hashOps, sourceProtocolVersion.unwrap)
            )
          )
        unassignmentResultEvent <- DeliveredUnassignmentResult
          .create(NoOpeningErrors(unassignmentResultEventMC))
          .leftMap(err => OtherError(err.toString))
      } yield CommonData(
        salt,
        unassignmentResultEvent,
        sourceProtocolVersion,
      )
  }
}

/** A fully unblinded [[AssignmentViewTree]]
  *
  * @throws java.lang.IllegalArgumentException if the [[tree]] is not fully unblinded
  */
final case class FullAssignmentTree(tree: AssignmentViewTree)
    extends FullReassignmentViewTree
    with HasToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "an assignment request must be fully unblinded")

  protected[this] val commonData: AssignmentCommonData = tree.commonData.tryUnwrap
  protected[this] val view: AssignmentView = tree.view.tryUnwrap

  override def reassignmentId: Option[ReassignmentId] = Some(
    view.unassignmentResultEvent.reassignmentId
  )

  def unassignmentResultEvent: DeliveredUnassignmentResult = view.unassignmentResultEvent

  def mediatorMessage(
      submittingParticipantSignature: Signature
  ): AssignmentMediatorMessage = tree.mediatorMessage(submittingParticipantSignature)

  // Domains
  override def sourceDomain: Source[DomainId] =
    view.unassignmentResultEvent.reassignmentId.sourceDomain
  override def targetDomain: Target[DomainId] = commonData.targetDomain
  override def domainId: DomainId = commonData.targetDomain.unwrap
  override def mediator: MediatorGroupRecipient = commonData.targetMediatorGroup

  override def toBeSigned: Option[RootHash] = Some(tree.rootHash)

  override def viewHash: ViewHash = tree.viewHash

  override def rootHash: RootHash = tree.rootHash

  override protected def pretty: Pretty[FullAssignmentTree] = prettyOfClass(unnamedParam(_.tree))

  override def toByteString: ByteString = tree.toByteString
}

object FullAssignmentTree {
  def fromByteString(
      crypto: CryptoPureApi,
      targetProtocolVersion: Target[ProtocolVersion],
  )(bytes: ByteString): ParsingResult[FullAssignmentTree] =
    for {
      tree <- AssignmentViewTree.fromByteString(crypto, targetProtocolVersion)(bytes)
      _ <- Either.cond(
        tree.isFullyUnblinded,
        (),
        OtherError(s"Assignment request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullAssignmentTree(tree)
}
