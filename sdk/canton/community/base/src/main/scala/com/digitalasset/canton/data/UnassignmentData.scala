// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.{
  ContractDeserializationError,
  InvariantViolation,
}
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.protocol.{ContractInstance, ReassignmentId, Stakeholders, v30}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.*

/** Stores the data of an unassignment that needs to be passed from the source synchronizer to the
  * target synchronizer.
  */
final case class UnassignmentData(
    // Data known by the submitter
    submitterMetadata: ReassignmentSubmitterMetadata,
    contractsBatch: ContractsReassignmentBatch,
    reassigningParticipants: Set[ParticipantId],
    sourcePSId: Source[PhysicalSynchronizerId],
    targetPSId: Target[PhysicalSynchronizerId],
    targetTimestamp: Target[CantonTimestamp],
    // Data unknown by the submitter
    unassignmentTs: CantonTimestamp,
) extends HasProtocolVersionedWrapper[UnassignmentData] {
  @transient override protected lazy val companionObj: UnassignmentData.type = UnassignmentData

  override val representativeProtocolVersion: RepresentativeProtocolVersion[
    UnassignmentData.type
  ] = UnassignmentData.protocolVersionRepresentativeFor(sourcePSId.unwrap.protocolVersion)

  lazy val reassignmentId: ReassignmentId = ReassignmentId(
    sourcePSId.map(_.logical),
    targetPSId.map(_.logical),
    unassignmentTs,
    contractsBatch.contractIdCounters,
  )

  def stakeholders: Stakeholders = contractsBatch.stakeholders

  def toProtoV30: v30.UnassignmentData = v30.UnassignmentData(
    submitterMetadata = submitterMetadata.toProtoV30.some,
    contracts = contractsBatch.contracts.map { reassign =>
      com.digitalasset.canton.protocol.v30.ActiveContract(
        reassign.contract.encoded,
        reassign.counter.toProtoPrimitive,
      )
    },
    reassigningParticipantUids = reassigningParticipants.map(_.uid.toProtoPrimitive).toSeq,
    sourcePhysicalSynchronizerId = sourcePSId.unwrap.toProtoPrimitive,
    targetPhysicalSynchronizerId = targetPSId.unwrap.toProtoPrimitive,
    targetTimestamp = targetTimestamp.unwrap.toProtoTimestamp.some,
    unassignmentTs = unassignmentTs.toProtoTimestamp.some,
  )
}

object UnassignmentData
    extends VersioningCompanion[UnassignmentData]
    with ProtocolVersionedCompanionDbHelpers[UnassignmentData] {

  override def name: String = "UnassignmentData"
  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v34), v30.UnassignmentData)(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30,
      )
  )

  def apply(
      unassignmentRequest: FullUnassignmentTree,
      unassignmentTs: CantonTimestamp,
  ): UnassignmentData = UnassignmentData(
    submitterMetadata = unassignmentRequest.submitterMetadata,
    contractsBatch = unassignmentRequest.contracts,
    reassigningParticipants = unassignmentRequest.reassigningParticipants,
    sourcePSId = unassignmentRequest.sourceSynchronizer,
    targetPSId = unassignmentRequest.targetSynchronizer,
    unassignmentTs = unassignmentTs,
    targetTimestamp = unassignmentRequest.targetTimestamp,
  )

  private def fromProtoV30(proto: v30.UnassignmentData): ParsingResult[UnassignmentData] = for {
    submitterMetadata <- ProtoConverter.parseRequired(
      ReassignmentSubmitterMetadata.fromProtoV30,
      "submitter_metadata",
      proto.submitterMetadata,
    )

    contracts <- proto.contracts
      .traverse {
        case com.digitalasset.canton.protocol.v30.ActiveContract(contractP, reassignmentCounterP) =>
          ContractInstance
            .decodeWithCreatedAt(contractP)
            .leftMap(err => ContractDeserializationError(err))
            .map(c =>
              (
                c,
                Source(c.templateId.packageId),
                Target(c.templateId.packageId),
                ReassignmentCounter(reassignmentCounterP),
              )
            )
      }
      .flatMap(
        ContractsReassignmentBatch
          .create(_)
          .leftMap(err => InvariantViolation(Some("contracts"), err.toString))
      )

    reassigningParticipants <- proto.reassigningParticipantUids
      .traverse(uid =>
        UniqueIdentifier.fromProtoPrimitive(uid, "reassigning_participants").map(ParticipantId(_))
      )

    sourceSynchronizer <- PhysicalSynchronizerId
      .fromProtoPrimitive(
        proto.sourcePhysicalSynchronizerId,
        "source_physical_synchronizer_id",
      )
      .map(Source(_))

    targetSynchronizer <- PhysicalSynchronizerId
      .fromProtoPrimitive(
        proto.targetPhysicalSynchronizerId,
        "target_physical_synchronizer_id",
      )
      .map(Target(_))

    targetTimestamp <- ProtoConverter
      .parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "target_timestamp",
        proto.targetTimestamp,
      )
      .map(Target(_))

    unassignmentTs <- ProtoConverter.parseRequired(
      CantonTimestamp.fromProtoTimestamp,
      "unassignment_ts",
      proto.unassignmentTs,
    )
  } yield new UnassignmentData(
    submitterMetadata = submitterMetadata,
    contractsBatch = contracts,
    reassigningParticipants = reassigningParticipants.toSet,
    sourcePSId = sourceSynchronizer,
    targetPSId = targetSynchronizer,
    targetTimestamp = targetTimestamp,
    unassignmentTs = unassignmentTs,
  )

  sealed trait ReassignmentGlobalOffset extends Product with Serializable {
    def merge(other: ReassignmentGlobalOffset): Either[String, ReassignmentGlobalOffset]

    def unassignment: Option[Offset]
    def assignment: Option[Offset]
  }

  object ReassignmentGlobalOffset {
    def create(
        unassignment: Option[Offset],
        assignment: Option[Offset],
    ): Either[String, Option[ReassignmentGlobalOffset]] =
      (unassignment, assignment) match {
        case (Some(unassignment), Some(assignment)) =>
          ReassignmentGlobalOffsets.create(unassignment, assignment).map(Some(_))
        case (Some(unassignment), None) => Right(Some(UnassignmentGlobalOffset(unassignment)))
        case (None, Some(assignment)) => Right(Some(AssignmentGlobalOffset(assignment)))
        case (None, None) => Right(None)
      }
  }

  final case class UnassignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            offset == newUnassignment,
            this,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          ReassignmentGlobalOffsets.create(offset, newAssignment)
        case offsets @ ReassignmentGlobalOffsets(newUnassignment, _) =>
          Either.cond(
            offset == newUnassignment,
            offsets,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
      }

    override def unassignment: Option[Offset] = Some(offset)
    override def assignment: Option[Offset] = None
  }

  final case class AssignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            offset == newAssignment,
            this,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
        case UnassignmentGlobalOffset(newUnassignment) =>
          ReassignmentGlobalOffsets.create(newUnassignment, offset)
        case offsets @ ReassignmentGlobalOffsets(_, newAssignment) =>
          Either.cond(
            offset == newAssignment,
            offsets,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
      }

    override def unassignment: Option[Offset] = None
    override def assignment: Option[Offset] = Some(offset)
  }

  final case class ReassignmentGlobalOffsets private (
      unassignmentOffset: Offset,
      assignmentOffset: Offset,
  ) extends ReassignmentGlobalOffset {
    require(
      unassignmentOffset != assignmentOffset,
      s"Unassignment and assignment offsets should be different; got $unassignmentOffset",
    )

    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset,
            this,
            s"Unable to merge unassignment offsets $unassignment and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            newAssignment == assignmentOffset,
            this,
            s"Unable to merge assignment offsets $assignment and $newAssignment",
          )
        case ReassignmentGlobalOffsets(newUnassignment, newAssignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset && newAssignment == assignmentOffset,
            this,
            s"Unable to merge reassignment offsets ($unassignment, $assignment) and ($newUnassignment, $newAssignment)",
          )
      }

    override def unassignment: Option[Offset] = Some(unassignmentOffset)
    override def assignment: Option[Offset] = Some(assignmentOffset)
  }

  object ReassignmentGlobalOffsets {
    def create(
        unassignment: Offset,
        assignment: Offset,
    ): Either[String, ReassignmentGlobalOffsets] =
      Either.cond(
        unassignment != assignment,
        ReassignmentGlobalOffsets(unassignment, assignment),
        s"Unassignment and assignment offsets should be different but got $unassignment",
      )
  }
}
