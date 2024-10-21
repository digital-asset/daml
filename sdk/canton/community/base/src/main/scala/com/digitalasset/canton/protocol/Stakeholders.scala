// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

// Invariant: signatories is a subset of stakeholders
final case class Stakeholders private (all: Set[LfPartyId])
    extends HasVersionedWrapper[Stakeholders]
    with PrettyPrinting {

  override protected def companionObj: HasVersionedMessageCompanionCommon[Stakeholders] =
    Stakeholders

  override protected def pretty: Pretty[Stakeholders.this.type] = prettyOfClass(
    param("all", _.all)
  )

  def toProtoV30: v30.Stakeholders = v30.Stakeholders(
    all = all.toSeq
  )
}

object Stakeholders extends HasVersionedMessageCompanion[Stakeholders] {
  override def name: String = "Stakeholders"
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.Stakeholders)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def tryCreate(stakeholders: Set[LfPartyId]): Stakeholders =
    new Stakeholders(all = stakeholders)

  def apply(metadata: ContractMetadata): Stakeholders =
    Stakeholders(all = metadata.stakeholders)

  def fromProtoV30(stakeholdersP: v30.Stakeholders): ParsingResult[Stakeholders] =
    for {
      stakeholders <- stakeholdersP.all
        .traverse(ProtoConverter.parseLfPartyId(_, "stakeholders"))
        .map(_.toSet)

    } yield Stakeholders(all = stakeholders)

}
