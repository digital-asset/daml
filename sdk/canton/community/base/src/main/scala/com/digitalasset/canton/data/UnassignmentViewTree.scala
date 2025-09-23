// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.toTraverseOps
import com.digitalasset.canton.ProtoDeserializationError.{
  ContractDeserializationError,
  InvariantViolation,
  OtherError,
}
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.RevealSubtree
import com.digitalasset.canton.data.ReassignmentRef.ContractIdRef
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.UnassignmentMediatorMessage
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*
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
    with HasProtocolVersionedWrapper[UnassignmentViewTree]
    with ReassignmentViewTree {

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
      protocolVersion: ProtocolVersion,
  ): UnassignmentMediatorMessage =
    UnassignmentMediatorMessage(blindedTree, submittingParticipantSignature)(
      UnassignmentMediatorMessage.protocolVersionRepresentativeFor(protocolVersion)
    )

  override protected def pretty: Pretty[UnassignmentViewTree] = prettyOfClass(
    param("common data", _.commonData),
    param("view", _.view),
  )

  @transient override protected lazy val companionObj: UnassignmentViewTree.type =
    UnassignmentViewTree
}

object UnassignmentViewTree
    extends VersioningCompanionContext[
      UnassignmentViewTree,
      (HashOps, Source[ProtocolVersionValidation]),
    ] {

  override val name: String = "UnassignmentViewTree"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ReassignmentViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30,
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

  def fromProtoV30(context: (HashOps, Source[ProtocolVersionValidation]))(
      unassignmentViewTreeP: v30.ReassignmentViewTree
  ): ParsingResult[UnassignmentViewTree] = {
    val (hashOps, expectedProtocolVersion) = context

    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      res <- GenReassignmentViewTree.fromProtoV30(
        UnassignmentCommonData.fromByteString(expectedProtocolVersion.unwrap, hashOps, _),
        UnassignmentView.fromByteString(expectedProtocolVersion.unwrap, hashOps, _),
      )((commonData, view) =>
        UnassignmentViewTree(commonData, view)(
          rpv,
          hashOps,
        )
      )(unassignmentViewTreeP)
    } yield res

  }
}

/** Aggregates the data of an unassignment request that is sent to the mediator and the involved
  * participants.
  *
  * @param salt
  *   Salt for blinding the Merkle hash
  * @param sourceSynchronizerId
  *   The synchronizer to which the unassignment request is sent
  * @param sourceMediatorGroup
  *   The mediator that coordinates the unassignment request on the source synchronizer
  * @param stakeholders
  *   Information about the stakeholders and signatories
  * @param reassigningParticipants
  *   The list of reassigning participants
  * @param uuid
  *   The request UUID of the unassignment
  * @param submitterMetadata
  *   information about the submission
  */
final case class UnassignmentCommonData private (
    override val salt: Salt,
    sourceSynchronizerId: Source[PhysicalSynchronizerId],
    sourceMediatorGroup: MediatorGroupRecipient,
    stakeholders: Stakeholders,
    reassigningParticipants: Set[ParticipantId],
    uuid: UUID,
    submitterMetadata: ReassignmentSubmitterMetadata,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      UnassignmentCommonData.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[UnassignmentCommonData](hashOps)
    with ReassignmentCommonData
    with HasProtocolVersionedWrapper[UnassignmentCommonData] {

  @transient override protected lazy val companionObj: UnassignmentCommonData.type =
    UnassignmentCommonData

  protected def toProtoV30: v30.UnassignmentCommonData =
    v30.UnassignmentCommonData(
      salt = Some(salt.toProtoV30),
      sourcePhysicalSynchronizerId = sourceSynchronizerId.unwrap.toProtoPrimitive,
      sourceMediatorGroup = sourceMediatorGroup.group.value,
      stakeholders = Some(stakeholders.toProtoV30),
      uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
      submitterMetadata = Some(submitterMetadata.toProtoV30),
      reassigningParticipantUids = reassigningParticipants.toSeq.map(_.uid.toProtoPrimitive),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.UnassignmentCommonData

  override protected def pretty: Pretty[UnassignmentCommonData] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("source synchronizer id", _.sourceSynchronizerId),
    param("source mediator group", _.sourceMediatorGroup),
    param("stakeholders", _.stakeholders),
    param("reassigning participants", _.reassigningParticipants),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )
}

object UnassignmentCommonData
    extends VersioningCompanionContextMemoization[
      UnassignmentCommonData,
      HashOps,
    ] {
  override val name: String = "UnassignmentCommonData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.UnassignmentCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      sourceSynchronizer: Source[PhysicalSynchronizerId],
      sourceMediatorGroup: MediatorGroupRecipient,
      stakeholders: Stakeholders,
      reassigningParticipants: Set[ParticipantId],
      uuid: UUID,
      submitterMetadata: ReassignmentSubmitterMetadata,
      sourceProtocolVersion: Source[ProtocolVersion],
  ): UnassignmentCommonData = UnassignmentCommonData(
    salt = salt,
    sourceSynchronizerId = sourceSynchronizer,
    sourceMediatorGroup = sourceMediatorGroup,
    stakeholders = stakeholders,
    reassigningParticipants = reassigningParticipants,
    uuid = uuid,
    submitterMetadata = submitterMetadata,
  )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.value), None)

  private[this] def fromProtoV30(
      hashOps: HashOps,
      unassignmentCommonDataP: v30.UnassignmentCommonData,
  )(
      bytes: ByteString
  ): ParsingResult[UnassignmentCommonData] = {
    val v30.UnassignmentCommonData(
      saltP,
      sourceSynchronizerP,
      stakeholdersP,
      reassigningParticipantUidsP,
      uuidP,
      sourceMediatorGroupP,
      submitterMetadataPO,
    ) = unassignmentCommonDataP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      sourceSynchronizerId <- PhysicalSynchronizerId
        .fromProtoPrimitive(sourceSynchronizerP, "source_physical_synchronizer_id")
        .map(Source(_))
      sourceMediatorGroup <- ProtoConverter.parseNonNegativeInt(
        "source_mediator_group",
        sourceMediatorGroupP,
      )

      stakeholders <- ProtoConverter.parseRequired(
        Stakeholders.fromProtoV30,
        "stakeholders",
        stakeholdersP,
      )
      reassigningParticipants <- reassigningParticipantUidsP.traverse(uid =>
        UniqueIdentifier
          .fromProtoPrimitive(uid, "reassigning_participant_uids")
          .map(ParticipantId(_))
      )

      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP)
      submitterMetadata <- ProtoConverter
        .required("submitter_metadata", submitterMetadataPO)
        .flatMap(ReassignmentSubmitterMetadata.fromProtoV30)

      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield UnassignmentCommonData(
      salt,
      sourceSynchronizerId,
      MediatorGroupRecipient(sourceMediatorGroup),
      stakeholders = stakeholders,
      reassigningParticipants = reassigningParticipants.toSet,
      uuid,
      submitterMetadata,
    )(hashOps, rpv, Some(bytes))
  }
}

/** Aggregates the data of an unassignment request that is only sent to the involved participants
  */
/** @param salt
  *   The salt used to blind the Merkle hash.
  * @param contract
  *   Contract being reassigned
  * @param targetSynchronizerId
  *   The synchronizer to which the contract is reassigned.
  * @param targetTimestamp
  *   The timestamp of the topology on the target synchronizer to use for validations.
  * @param targetProtocolVersion
  *   Protocol version of the target synchronizer
  */
final case class UnassignmentView private (
    override val salt: Salt,
    contracts: ContractsReassignmentBatch,
    targetSynchronizerId: Target[PhysicalSynchronizerId],
    targetTimestamp: Target[CantonTimestamp],
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      UnassignmentView.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[UnassignmentView](hashOps)
    with HasProtocolVersionedWrapper[UnassignmentView]
    with ReassignmentView {

  @transient override protected lazy val companionObj: UnassignmentView.type = UnassignmentView

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  def hashPurpose: HashPurpose = HashPurpose.UnassignmentView

  protected def toProtoV30: v30.UnassignmentView =
    v30.UnassignmentView(
      salt = Some(salt.toProtoV30),
      targetPhysicalSynchronizerId = targetSynchronizerId.unwrap.toProtoPrimitive,
      targetTimestamp = targetTimestamp.unwrap.toProtoPrimitive,
      contracts = contracts.contracts.map { reassign =>
        v30.ActiveContract(
          reassign.contract.encoded,
          reassign.counter.toProtoPrimitive,
        )
      },
    )

  override protected def pretty: Pretty[UnassignmentView] = prettyOfClass(
    param("template ids", _.contracts.contracts.map(_.templateId).toSet),
    param("target synchronizer id", _.targetSynchronizerId),
    param("target timestamp", _.targetTimestamp),
    param(
      "contracts",
      _.contracts.contractIds,
    ), // do not log contract details because it contains confidential data
    param("salt", _.salt),
  )
}

object UnassignmentView extends VersioningCompanionContextMemoization[UnassignmentView, HashOps] {
  override val name: String = "UnassignmentView"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.UnassignmentView)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(hashOps: HashOps)(
      salt: Salt,
      contracts: ContractsReassignmentBatch,
      targetSynchronizer: Target[PhysicalSynchronizerId],
      targetTimestamp: Target[CantonTimestamp],
      sourceProtocolVersion: Source[ProtocolVersion],
  ): UnassignmentView =
    UnassignmentView(
      salt,
      contracts,
      targetSynchronizer,
      targetTimestamp,
    )(hashOps, protocolVersionRepresentativeFor(sourceProtocolVersion.unwrap), None)

  private[this] def fromProtoV30(hashOps: HashOps, unassignmentViewP: v30.UnassignmentView)(
      bytes: ByteString
  ): ParsingResult[UnassignmentView] = {
    val v30.UnassignmentView(
      saltP,
      targetSynchronizerIdP,
      targetTimestampP,
      contractsP,
    ) = unassignmentViewP

    for {
      salt <- ProtoConverter.parseRequired(Salt.fromProtoV30, "salt", saltP)
      targetSynchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        targetSynchronizerIdP,
        "targetPhysicalSynchronizerId",
      )
      targetTimestamp <- CantonTimestamp.fromProtoPrimitive(targetTimestampP)
      contracts <- contractsP
        .traverse { case v30.ActiveContract(contractP, reassignmentCounterP) =>
          ContractInstance
            .decodeWithCreatedAt(contractP)
            .leftMap(err => ContractDeserializationError(err))
            .map(_ -> ReassignmentCounter(reassignmentCounterP))
        }
        .flatMap(
          ContractsReassignmentBatch
            .create(_)
            .leftMap(err => InvariantViolation(Some("contracts"), err.toString))
        )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield UnassignmentView(
      salt,
      contracts,
      Target(targetSynchronizerId),
      Target(targetTimestamp),
    )(
      hashOps,
      rpv,
      Some(bytes),
    )

  }
}

/** A fully unblinded [[UnassignmentViewTree]]
  *
  * @throws java.lang.IllegalArgumentException
  *   if the [[tree]] is not fully unblinded
  */
final case class FullUnassignmentTree(tree: UnassignmentViewTree)
    extends FullReassignmentViewTree
    with HasToByteString
    with PrettyPrinting {
  require(tree.isFullyUnblinded, "An unassignment request must be fully unblinded")

  protected[this] val commonData: UnassignmentCommonData = tree.commonData.tryUnwrap
  protected[this] val view: UnassignmentView = tree.view.tryUnwrap

  override def reassignmentRef: ContractIdRef = ContractIdRef(contracts.contractIds.toSet)

  // Synchronizers
  override def synchronizerId: PhysicalSynchronizerId = commonData.sourceSynchronizerId.unwrap
  override def sourceSynchronizer: Source[PhysicalSynchronizerId] = commonData.sourceSynchronizerId
  override def targetSynchronizer: Target[PhysicalSynchronizerId] = view.targetSynchronizerId
  def targetTimestamp: Target[CantonTimestamp] = view.targetTimestamp
  def targetProtocolVersion: Target[ProtocolVersion] = targetSynchronizer.map(_.protocolVersion)

  def mediatorMessage(
      submittingParticipantSignature: Signature,
      protocolVersion: Source[ProtocolVersion],
  ): UnassignmentMediatorMessage =
    tree.mediatorMessage(submittingParticipantSignature, protocolVersion.value)

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
      expectedProtocolVersion: Source[ProtocolVersionValidation],
  )(bytes: ByteString): ParsingResult[FullUnassignmentTree] =
    for {
      tree <- UnassignmentViewTree.fromByteString(
        expectedProtocolVersion.value,
        (crypto, expectedProtocolVersion),
        bytes,
      )
      _ <- Either.cond(
        tree.isFullyUnblinded,
        (),
        OtherError(s"Unassignment request ${tree.rootHash} is not fully unblinded"),
      )
    } yield FullUnassignmentTree(tree)
}
