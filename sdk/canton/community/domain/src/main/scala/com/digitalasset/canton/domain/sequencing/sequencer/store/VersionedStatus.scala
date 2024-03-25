// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.rpc.status.Status

final case class VersionedStatus private (status: Status)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[VersionedStatus.type]
) extends HasProtocolVersionedWrapper[VersionedStatus] {

  override protected val companionObj: VersionedStatus.type = VersionedStatus
  def toProtoV30: v30.VersionedStatus = v30.VersionedStatus(Some(status))
}

object VersionedStatus extends HasProtocolVersionedCompanion2[VersionedStatus, VersionedStatus] {

  /** The name of the class as used for pretty-printing and error reporting */
  override def name: String = "VersionedStatus"

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter
      .storage(
        ReleaseProtocolVersion(ProtocolVersion.v30),
        v30.VersionedStatus.messageCompanion,
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
  )

  def fromProtoV30(versionedStatusP: v30.VersionedStatus): ParsingResult[VersionedStatus] = {
    for {
      status <- versionedStatusP.status.toRight(ProtoDeserializationError.FieldNotSet("status"))
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield VersionedStatus(status)(rpv)
  }

  def create(status: Status, protocolVersion: ProtocolVersion): VersionedStatus = {
    VersionedStatus(status)(protocolVersionRepresentativeFor(protocolVersion))
  }
}
