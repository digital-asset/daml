// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.implicits.toTraverseOps
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.*

final case class ExternalAuthorization(
    signatures: Map[PartyId, Seq[Signature]],
    hashingSchemeVersion: HashingSchemeVersion,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ExternalAuthorization.type
    ]
) extends HasProtocolVersionedWrapper[ExternalAuthorization]
    with PrettyPrinting {

  override protected def pretty: Pretty[ExternalAuthorization] = prettyOfClass(
    param("signatures", _.signatures)
  )

  private[canton] def toProtoV30: v30.ExternalAuthorization =
    v30.ExternalAuthorization(
      authentications = signatures.map { case (party, partySignatures) =>
        v30.ExternalPartyAuthorization(party.toProtoPrimitive, partySignatures.map(_.toProtoV30))
      }.toSeq,
      hashingSchemeVersion.toProtoV30,
    )

  @transient override protected lazy val companionObj: ExternalAuthorization.type =
    ExternalAuthorization

}

object ExternalAuthorization
    extends HasProtocolVersionedCompanion[ExternalAuthorization]
    with ProtocolVersionedCompanionDbHelpers[ExternalAuthorization] {

  def create(
      signatures: Map[PartyId, Seq[Signature]],
      hashingSchemeVersion: HashingSchemeVersion,
      protocolVersion: ProtocolVersion,
  ): ExternalAuthorization =
    ExternalAuthorization(signatures, hashingSchemeVersion)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "ExternalAuthorization"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(protoCompanion =
      v30.ExternalAuthorization
    )(supportedProtoVersion(_)(fromProtoV30), _.toProtoV30)
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
    } yield create(signatures.toMap, hashingSchemeVersion, rpv.representative)
  }

}
