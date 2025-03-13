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
final case class ActiveContractNew( // TODO(#24326) - Replaces current ActiveContract (which is going to be removed)
    contract: LapiActiveContract
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[ActiveContractNew.type])
    extends HasProtocolVersionedWrapper[ActiveContractNew] {

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def toProtoV30: v30.ActiveContractNew =
    v30.ActiveContractNew(
      // Fine to call toByteString because it's a LAPI contract which does not use the versioning tooling
      contract.toByteString
    )

  override protected lazy val companionObj: ActiveContractNew.type = ActiveContractNew

}

object ActiveContractNew extends VersioningCompanion[ActiveContractNew] {

  override def name: String = "ActiveContractNew" // TODO(#24326) - Update name

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.ActiveContractNew)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def fromProtoV30(
      proto: v30.ActiveContractNew
  ): ParsingResult[ActiveContractNew] =
    for {
      contract <- ProtoConverter.protoParser(LapiActiveContract.parseFrom)(
        proto.activeContract
      )
      reprProtocolVersion <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield ActiveContractNew(contract)(reprProtocolVersion)

  def tryCreate(
      contract: LapiActiveContract
  ): ActiveContractNew = {
    val converters = ActiveContractNew.versioningTable.converters
    // Assumption: The probability that we need a second version is quite low
    if (converters.sizeIs != 1) {
      throw new IllegalStateException("Only one protocol version is supported for ACS export")
    }
    val rpv = converters.headOption
      .getOrElse(throw new IllegalStateException("Versioning table converters are empty"))
      ._2
      .fromInclusive
    ActiveContractNew(contract)(rpv)
  }

}
