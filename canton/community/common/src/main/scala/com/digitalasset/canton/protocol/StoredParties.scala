// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}

import scala.collection.immutable.SortedSet

// TODO(#3256) get rid of, or at least simplify this; using an array would also allow us to remove the stakeholders_hash column in the commitment_snapshot table
final case class StoredParties(parties: SortedSet[LfPartyId])
    extends HasVersionedWrapper[StoredParties] {

  override protected def companionObj = StoredParties

  protected def toProtoV30: v30.StoredParties = v30.StoredParties(parties.toList)
}

object StoredParties
    extends HasVersionedMessageCompanion[StoredParties]
    with HasVersionedMessageCompanionDbHelpers[StoredParties] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.StoredParties)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def fromIterable(parties: Iterable[LfPartyId]): StoredParties = StoredParties(
    SortedSet.from(parties)
  )

  override def name: String = "stored parties"

  def fromProtoV30(proto0: v30.StoredParties): ParsingResult[StoredParties] = {
    val v30.StoredParties(partiesP) = proto0
    for {
      parties <- partiesP.traverse(ProtoConverter.parseLfPartyId)
    } yield StoredParties.fromIterable(parties)
  }
}
