// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.implicits.toBifunctorOps
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  LfContractId,
  ReassignmentId,
  SerializableContract,
  TransactionId,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.digitalasset.canton.{ProtoDeserializationError, ReassignmentCounter}
import com.digitalasset.daml.lf.data.Bytes;

final case class CommitmentContractMetadata(
    cid: LfContractId,
    reassignmentCounter: ReassignmentCounter,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      CommitmentContractMetadata.type
    ]
) extends HasProtocolVersionedWrapper[CommitmentContractMetadata]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: CommitmentContractMetadata.type =
    CommitmentContractMetadata
  override protected def pretty: Pretty[CommitmentContractMetadata.this.type] =
    prettyOfClass(
      param("contract id", _.cid),
      param("reassignment counter", _.reassignmentCounter.v),
    )

  private def toProtoV30: v30.CommitmentContractMeta = v30.CommitmentContractMeta(
    cid.toBytes.toByteString,
    reassignmentCounter.v,
  )
}

object CommitmentContractMetadata
    extends HasProtocolVersionedCompanion[
      CommitmentContractMetadata,
    ] {

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(v30.CommitmentContractMeta)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      contract: v30.CommitmentContractMeta
  ): ParsingResult[CommitmentContractMetadata] =
    for {
      cid <- LfContractId
        .fromBytes(Bytes.fromByteString(contract.cid))
        .leftMap(ProtoDeserializationError.StringConversionError.apply(_, field = Some("cid")))
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield CommitmentContractMetadata(cid, ReassignmentCounter(contract.reassignmentCounter))(
      reprProtocolVersion
    )

  override def name: String = "commitment contract metadata"

  def create(
      cid: LfContractId,
      reassignmentCounter: ReassignmentCounter,
  )(protocolVersion: ProtocolVersion): CommitmentContractMetadata =
    CommitmentContractMetadata(cid, reassignmentCounter)(
      protocolVersionRepresentativeFor(protocolVersion)
    )
}

final case class CommitmentInspectContract(
    contract: SerializableContract,
    creatingTxId: TransactionId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      CommitmentInspectContract.type
    ]
) extends HasProtocolVersionedWrapper[CommitmentInspectContract]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: CommitmentInspectContract.type =
    CommitmentInspectContract

  override protected def pretty: Pretty[CommitmentInspectContract.this.type] =
    prettyOfClass(
      param("contract", _.contract),
      param("creating tx id", _.creatingTxId),
    )

  private def toProtoV30: v30.CommitmentContract = v30.CommitmentContract(
    Some(contract.toAdminProtoV30),
    creatingTxId.toProtoPrimitive,
  )
}

object CommitmentInspectContract extends HasProtocolVersionedCompanion[CommitmentInspectContract] {

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(v30.CommitmentContract)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      cmtContract: v30.CommitmentContract
  ): ParsingResult[CommitmentInspectContract] =
    for {
      contract <- ProtoConverter.parseRequired(
        SerializableContract.fromAdminProtoV30,
        "contract",
        cmtContract.serializedContract,
      )
      creatingTxId <- TransactionId.fromProtoPrimitive(cmtContract.creatingTxId)
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield CommitmentInspectContract(contract, creatingTxId)(reprProtocolVersion)

  override def name: String = "commitment inspect contract"
}

final case class CommitmentMismatchInfo(
    // This is the reference domain for contract mismatch info
    domain: DomainId,
    timestamp: CantonTimestamp,
    participant: ParticipantId,
    counterParticipant: ParticipantId,
    mismatches: Seq[ContractMismatchInfo],
) extends PrettyPrinting {
  override protected def pretty: Pretty[CommitmentMismatchInfo] = prettyOfClass(
    param("domain", _.domain),
    param("domain mismatch timestamp", _.timestamp),
    param("participant", _.participant),
    param("counter-participant", _.counterParticipant),
    param("mismatching contracts", _.mismatches),
  )
}

final case class ContractMismatchInfo(
    contract: SerializableContract,
    reason: MismatchReason,
) extends PrettyPrinting {
  override protected def pretty: Pretty[ContractMismatchInfo] = prettyOfClass(
    param("contract", _.contract),
    param("mismatch reason", _.reason),
  )
}

sealed trait MismatchReason extends Product with Serializable with PrettyPrinting

final case class InexistentContract(
    participantWithContract: ParticipantId,
    active: ContractActive,
    participantWithoutContract: ParticipantId,
) extends MismatchReason {
  override protected def pretty: Pretty[InexistentContract] = prettyOfClass(
    param("Contract exists on participant", _.participantWithContract),
    param("activated by", _.active),
    param("but does not exist on participant", _.participantWithoutContract),
  )
}

final case class DeactivatedContract(
    participantWithContract: ParticipantId,
    active: ContractActive,
    participantWithoutContract: ParticipantId,
    inactive: ContractInactive,
    whereActive: Option[ContractActive],
) extends MismatchReason
    with PrettyPrinting {
  override protected def pretty: Pretty[DeactivatedContract] = prettyOfClass(
    param("Contract exists on participant", _.participantWithContract),
    param("activated by", _.active),
    param("but on participant", _.participantWithoutContract),
    param("was deactivated by", _.inactive),
    paramIfDefined("and is now active on", _.whereActive),
  )
}

sealed trait ContractActive extends Product with Serializable with PrettyPrinting
sealed trait ContractInactive extends Product with Serializable with PrettyPrinting

final case class ContractCreated(domain: DomainId, creatingTxId: Option[TransactionId])
    extends ContractActive {
  override protected def pretty: Pretty[ContractCreated] = prettyOfClass(
    param("domain", _.domain),
    paramIfDefined("activation tx id", _.creatingTxId),
  )
}

final case class ContractAssigned(
    srcDomain: DomainId,
    targetDomain: DomainId,
    reassignmentCounterTarget: ReassignmentCounter,
    reassignmentId: Option[ReassignmentId],
) extends ContractActive {
  override protected def pretty: Pretty[ContractAssigned] = prettyOfClass(
    param("source domain", _.srcDomain),
    param("target domain", _.targetDomain),
    param("reassignment counter on target", _.reassignmentCounterTarget),
    paramIfDefined("reassignment id", _.reassignmentId),
  )
}

final case class ContractUnassigned(
    srcDomain: DomainId,
    targetDomain: DomainId,
    reassignmentCounterSrc: ReassignmentCounter,
    reassignmentId: Option[ReassignmentId],
) extends ContractInactive {
  override protected def pretty: Pretty[ContractUnassigned] = prettyOfClass(
    param("source domain", _.srcDomain),
    param("target domain", _.targetDomain),
    param("reassignment counter on source domain", _.reassignmentCounterSrc),
    paramIfDefined("reassignment id", _.reassignmentId),
  )
}

final case class ContractArchived(
    domain: DomainId,
    archivingTxId: Option[TransactionId],
    reassignmentCounter: ReassignmentCounter,
) extends ContractInactive {
  override protected def pretty: Pretty[ContractArchived] = prettyOfClass(
    param("domain", _.domain),
    param("reassignment counter", _.reassignmentCounter),
    paramIfDefined("archiving tx id", _.archivingTxId),
  )
}
