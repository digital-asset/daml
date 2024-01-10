// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.rpc.status.Status

final case class VersionedStatus private (status: Status)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[VersionedStatus.type]
) extends HasProtocolVersionedWrapper[VersionedStatus] {

  override protected val companionObj: VersionedStatus.type = VersionedStatus
  def toProtoV0: v0.VersionedStatus = v0.VersionedStatus(Some(status))
}

object VersionedStatus extends HasProtocolVersionedCompanion2[VersionedStatus, VersionedStatus] {

  /** The name of the class as used for pretty-printing and error reporting */
  override def name: String = "VersionedStatus"

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter
      .storage(
        ReleaseProtocolVersion(ProtocolVersion.v30),
        v0.VersionedStatus.messageCompanion,
      )(
        supportedProtoVersion(_)(fromProtoV0),
        _.toProtoV0.toByteString,
      )
  )

  def fromProtoV0(versionedStatusP: v0.VersionedStatus): ParsingResult[VersionedStatus] = {
    val protocolVersion = protocolVersionRepresentativeFor(ProtoVersion(0))
    for {
      status <- versionedStatusP.status.toRight(ProtoDeserializationError.FieldNotSet("status"))
    } yield VersionedStatus(status)(protocolVersion)
  }

  def create(status: Status, protocolVersion: ProtocolVersion): VersionedStatus = {
    VersionedStatus(status)(protocolVersionRepresentativeFor(protocolVersion))
  }
}
