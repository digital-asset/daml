// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.implicits.toTraverseOps
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30.ExternalPartyAuthorization
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.*

final case class ExternalAuthorization(
    signatures: Map[PartyId, Seq[Signature]],
    hashingSchemeVersion: HashingSchemeVersion,
    maxRecordTime: Option[CantonTimestamp],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ExternalAuthorization.type
    ]
) extends HasProtocolVersionedWrapper[ExternalAuthorization]
    with PrettyPrinting {

  override protected def pretty: Pretty[ExternalAuthorization] = prettyOfClass(
    param("signatures", _.signatures)
  )

  private def authenticationsV30: Seq[ExternalPartyAuthorization] =
    signatures.map { case (party, partySignatures) =>
      v30.ExternalPartyAuthorization(party.toProtoPrimitive, partySignatures.map(_.toProtoV30))
    }.toSeq

  private[canton] def toProtoV30: v30.ExternalAuthorization =
    // In PV34 max record time enforced via the max sequencing time
    v30.ExternalAuthorization(
      authentications = authenticationsV30,
      hashingSchemeVersion.toProtoV30,
    )

  private[canton] def toProtoV31: v31.ExternalAuthorization =
    v31.ExternalAuthorization(
      authentications = authenticationsV30,
      hashingSchemeVersion = hashingSchemeVersion.toProtoV31,
      maxRecordTime = maxRecordTime.map(_.toProtoPrimitive),
    )

  @transient override protected lazy val companionObj: ExternalAuthorization.type =
    ExternalAuthorization

}

object ExternalAuthorization
    extends VersioningCompanion[ExternalAuthorization]
    with ProtocolVersionedCompanionDbHelpers[ExternalAuthorization] {

  def create(
      signatures: Map[PartyId, Seq[Signature]],
      hashingSchemeVersion: HashingSchemeVersion,
      maxRecordTime: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
  ): ExternalAuthorization =
    ExternalAuthorization(signatures, hashingSchemeVersion, maxRecordTime)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "ExternalAuthorization"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(protoCompanion =
      v30.ExternalAuthorization
    )(supportedProtoVersion(_)(fromProtoV30), _.toProtoV30),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(protoCompanion =
      v31.ExternalAuthorization
    )(supportedProtoVersion(_)(fromProtoV31), _.toProtoV31),
  )

  private def fromProtoV30(
      proto: v30.ExternalPartyAuthorization
  ): ParsingResult[(PartyId, Seq[Signature])] = {
    val v30.ExternalPartyAuthorization(partyP, signaturesP) = proto
    for {
      partyId <- PartyId.fromProtoPrimitive(partyP, "party")
      partySignatures <- signaturesP.traverse(Signature.fromProtoV30)
    } yield partyId -> partySignatures
  }

  def fromProtoV30(
      proto: v30.ExternalAuthorization
  ): ParsingResult[ExternalAuthorization] = {
    val v30.ExternalAuthorization(signaturesP, _) = proto
    for {
      signatures <- signaturesP.traverse(fromProtoV30)
      hashingSchemeVersion <- HashingSchemeVersion.fromProtoV30(proto.hashingSchemeVersion)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield create(signatures.toMap, hashingSchemeVersion, None, rpv.representative)
  }

  def fromProtoV31(
      proto: v31.ExternalAuthorization
  ): ParsingResult[ExternalAuthorization] = {
    val v31.ExternalAuthorization(signaturesP, hashingSchemeVersionP, maxRecordTimeP) = proto
    for {
      signatures <- signaturesP.traverse(fromProtoV30)
      hashingSchemeVersion <- HashingSchemeVersion.fromProtoV31(hashingSchemeVersionP)
      maxRecordTime <- maxRecordTimeP.traverse(CantonTimestamp.fromProtoPrimitive)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
    } yield create(signatures.toMap, hashingSchemeVersion, maxRecordTime, rpv.representative)
  }

}
