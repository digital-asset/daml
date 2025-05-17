// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, OtherError}
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.data.ReassignmentRef.ReassignmentIdRef
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.AssignmentMediatorMessage
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
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
      protocolVersion: ProtocolVersion,
  ): AssignmentMediatorMessage =
    AssignmentMediatorMessage(blindedTree, submittingParticipantSignature)(
      AssignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion)
    )

  override protected def pretty: Pretty[AssignmentViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: AssignmentViewTree.type =
    AssignmentViewTree
}

object AssignmentViewTree
    extends VersioningCompanionContextTaggedPVValidation2[
      AssignmentViewTree,
      Target,
      HashOps,
    ] {

  override val name: String = "AssignmentViewTree"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ReassignmentViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30,
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
        AssignmentCommonData.fromByteString(expectedProtocolVersion.unwrap, hashOps),
        AssignmentView.fromByteString(expectedProtocolVersion.unwrap, hashOps),
      )((commonData, view) => AssignmentViewTree(commonData, view)(rpv, hashOps))(
        assignmentViewTreeP
      )
    } yield res
  }
}

/** Aggregates the data of an assignment request that is sent to the mediator and the involved
  * participants.
  *
  * @param salt
  *   Salt for blinding the Merkle hash
  * @param targetSynchronizerId
  *   The synchronizer on which the contract is assigned
  * @param targetMediatorGroup
  *   The mediator that coordinates the assignment request on the target synchronizer
  * @param stakeholders
  *   The stakeholders of the reassigned contract
  * @param uuid
  *   The uuid of the assignment request
  * @param submitterMetadata
  *   information about the submission
  * @param reassigningParticipants
  *   The list of reassigning participants
  */
final case class AssignmentCommonData private (
    override val salt: Salt,
    targetSynchronizerId: Target[PhysicalSynchronizerId],
    targetMediatorGroup: MediatorGroupRecipient,
    stakeholders: Stakeholders,
    uuid: UUID,
    submitterMetadata: ReassignmentSubmitterMetadata,
    reassigningParticipants: Set[ParticipantId],
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AssignmentCommonData.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[AssignmentCommonData](hashOps)
    with HasProtocolVersionedWrapper[AssignmentCommonData]
    with ReassignmentCommonData {

  @transient override protected lazy val companionObj: AssignmentCommonData.type =
    AssignmentCommonData

  protected def toProtoV30: v30.AssignmentCommonData =
    v30.AssignmentCommonData(
      salt = Some(salt.toProtoV30),
      targetPhysicalSynchronizerId = targetSynchronizerId.unwrap.toProtoPrimitive,
      targetMediatorGroup = targetMediatorGroup.group.value,
      stakeholders = Some(stakeholders.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
      reassigningParticipantUids = reassigningParticipants.map(_.uid.toProtoPrimitive).toSeq,
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.AssignmentCommonData

  override protected def pretty: Pretty[AssignmentCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("target synchronizer id", _.targetSynchronizerId),
    param("target mediator group", _.targetMediatorGroup),
    param("stakeholders", _.stakeholders),
    param("reassigning participants", _.reassigningParticipants),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object AssignmentCommonData
    extends VersioningCompanionContextMemoization[AssignmentCommonData, HashOps] {
  override val name: String = "AssignmentCommonData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.AssignmentCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      targetSynchronizer: Target[PhysicalSynchronizerId],
      targetMediatorGroup: MediatorGroupRecipient,
      stakeholders: Stakeholders,
      uuid: UUID,
      submitterMetadata: ReassignmentSubmitterMetadata,
      targetProtocolVersion: Target[
        ProtocolVersion
      ], // TODO(#25482) Reduce duplication in parameters
      reassigningParticipants: Set[ParticipantId],
  ): AssignmentCommonData = AssignmentCommonData(
    salt = salt,
    targetSynchronizerId = targetSynchronizer,
    targetMediatorGroup = targetMediatorGroup,
    stakeholders = stakeholders,
    uuid = uuid,
    submitterMetadata = submitterMetadata,
    reassigningParticipants = reassigningParticipants,
  )(hashOps, protocolVersionRepresentativeFor(targetProtocolVersion.unwrap), None)

  private[this] def fromProtoV30(
      hashOps: HashOps,
      assignmentCommonDataP: v30.AssignmentCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[AssignmentCommonData] = {
    val v30.AssignmentCommonData(
      saltP,
      targetSynchronizerP,
      stakeholdersP,
      uuidP,
      targetMediatorGroupP,
      submitterMetadataPO,
      reassigningParticipantsP,
    ) = assignmentCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetSynchronizerId <- PhysicalSynchronizerId
        .fromProtoPrimitive(targetSynchronizerP, "target_physical_synchronizer_id")
        .map(Target(_))
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

      reassigningParticipants <- reassigningParticipantsP.traverse(uid =>
        UniqueIdentifier
          .fromProtoPrimitive(uid, "reassigning_participant_uids")
          .map(ParticipantId(_))
      )

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield AssignmentCommonData(
      salt,
      targetSynchronizerId,
      MediatorGroupRecipient(targetMediatorGroup),
      stakeholders = stakeholders,
      uuid,
      submitterMetadata,
      reassigningParticipants = reassigningParticipants.toSet,
    )(hashOps, rpv, Some(bytes))
  }
}

/** Aggregates the data of an assignment request that is only sent to the involved participants
  *
  * @param salt
  *   The salt to blind the Merkle hash
  * @param contract
  *   The contract to be reassigned including the instance
  * @param unassignmentResultEvent
  *   The signed deliver event of the unassignment result message
  * @param reassignmentCounter
  *   The [[com.digitalasset.canton.ReassignmentCounter]] of the contract.
  */
final case class AssignmentView private (
    override val salt: Salt,
    reassignmentId: ReassignmentId,
    contracts: ContractsReassignmentBatch,
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
      contracts = contracts.contracts.map { case reassign =>
        v30.ActiveContract(
          Some(reassign.contract.toProtoV30),
          reassign.counter.toProtoPrimitive,
        )
      },
      reassignmentId = Some(reassignmentId.toProtoV30),
    )

  override protected def pretty: Pretty[AssignmentView] = prettyOfClass(
    param("reassignment id", _.reassignmentId),
    param(
      "contract ids and counters",
      _.contracts.contractIdCounters,
    ), // do not log contract details because it contains confidential data
    param("salt", _.salt),
  )
}

object AssignmentView extends VersioningCompanionContextMemoization[AssignmentView, HashOps] {
  override val name: String = "AssignmentView"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.AssignmentView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      reassignmentId: ReassignmentId,
      contracts: ContractsReassignmentBatch,
      targetProtocolVersion: Target[ProtocolVersion],
  ): Either[String, AssignmentView] = Either
    .catchOnly[IllegalArgumentException](
      AssignmentView(
        salt,
        reassignmentId,
        contracts,
      )(hashOps, protocolVersionRepresentativeFor(targetProtocolVersion.unwrap), None)
    )
    .leftMap(_.getMessage)

  private[this] def fromProtoV30(hashOps: HashOps, assignmentViewP: v30.AssignmentView)(
      bytes: ByteString
  ): ParsingResult[AssignmentView] = {
    val v30.AssignmentView(
      saltP,
      contractsP,
      reassignmentIdP,
    ) =
      assignmentViewP
    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      reassignmentId <- ProtoConverter
        .parseRequired(ReassignmentId.fromProtoV30, "reassignment_id", reassignmentIdP)
      contracts <- contractsP
        .traverse { case v30.ActiveContract(contractP, reassignmentCounterP) =>
          ProtoConverter
            .required("contract", contractP)
            .flatMap(SerializableContract.fromProtoV30)
            .map(_ -> ReassignmentCounter(reassignmentCounterP))
        }
        .flatMap(
          ContractsReassignmentBatch
            .create(_)
            .leftMap(err => InvariantViolation(Some("contracts"), err.toString))
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield AssignmentView(
      salt,
      reassignmentId,
      contracts,
    )(hashOps, rpv, Some(bytes))
  }
}

/** A fully unblinded [[AssignmentViewTree]]
  *
  * @throws java.lang.IllegalArgumentException
  *   if the [[tree]] is not fully unblinded
  */
final case class FullAssignmentTree(tree: AssignmentViewTree)
    extends FullReassignmentViewTree
    with HasToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "an assignment request must be fully unblinded")

  protected[this] val commonData: AssignmentCommonData = tree.commonData.tryUnwrap
  protected[this] val view: AssignmentView = tree.view.tryUnwrap

  def reassignmentId: ReassignmentId = view.reassignmentId

  override def reassignmentRef: ReassignmentIdRef = ReassignmentIdRef(reassignmentId)

  def mediatorMessage(
      submittingParticipantSignature: Signature,
      protocolVersion: Target[ProtocolVersion],
  ): AssignmentMediatorMessage =
    tree.mediatorMessage(submittingParticipantSignature, protocolVersion.unwrap)

  // Synchronizers
  override def sourceSynchronizer: Source[SynchronizerId] = reassignmentId.sourceSynchronizer
  override def targetSynchronizer: Target[PhysicalSynchronizerId] =
    commonData.targetSynchronizerId

  override def synchronizerId: PhysicalSynchronizerId = commonData.targetSynchronizerId.unwrap
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
