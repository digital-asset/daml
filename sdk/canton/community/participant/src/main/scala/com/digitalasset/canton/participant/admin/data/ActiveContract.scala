// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

/** Intended as small wrapper around a LAPI active contract, so that its use is versioned.
  */
final case class ActiveContract(
    contract: LapiActiveContract
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ActiveContract.type])
    extends HasProtocolVersionedWrapper[ActiveContract] {

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def toProtoV30: v30.ActiveContract =
    v30.ActiveContract(
      // Fine to call toByteString because it's a LAPI contract which does not use the versioning tooling
      contract.toByteString
    )

  override protected lazy val companionObj: ActiveContract.type = ActiveContract

}

object ActiveContract extends VersioningCompanion[ActiveContract] {

  override def name: String = "ActiveContract"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ActiveContract)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def fromProtoV30(
      proto: v30.ActiveContract
  ): ParsingResult[ActiveContract] =
    for {
      contract <- ProtoConverter.protoParser(LapiActiveContract.parseFrom)(
        proto.activeContract
      )
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ActiveContract(contract)(reprProtocolVersion)

  def tryCreate(
      contract: LapiActiveContract
  ): ActiveContract = {
    val converters = ActiveContract.versioningTable.converters
    // Assumption: The probability that we need a second version is quite low
    if (converters.sizeIs != 1) {
      throw new IllegalStateException("Only one protocol version is supported for ACS export")
    }
    val rpv = converters.headOption
      .getOrElse(throw new IllegalStateException("Versioning table converters are empty"))
      ._2
      .fromInclusive
    ActiveContract(contract)(rpv)
  }

}
