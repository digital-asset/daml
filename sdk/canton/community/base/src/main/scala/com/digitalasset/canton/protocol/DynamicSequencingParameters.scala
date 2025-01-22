// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Synchronizer-wide dynamic sequencing parameters.
  *
  * @param payload The opaque payload of the synchronizer-wide dynamic sequencing parameters;
  *                its content is sequencer-dependent and synchronizer owners are responsible
  *                for ensuring that it can be correctly interpreted by the sequencers in use.
  *                If no payload is provided, sequencer-specific default values are used.
  *                If the payload cannot be correctly interpreted or the parameters cannot
  *                be set due to dynamic conditions, their value will not change.
  */
final case class DynamicSequencingParameters(payload: Option[ByteString])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      DynamicSequencingParameters.type
    ]
) extends HasProtocolVersionedWrapper[DynamicSequencingParameters]
    with PrettyPrinting {

  @transient override protected lazy val companionObj: DynamicSequencingParameters.type =
    DynamicSequencingParameters

  override protected def pretty: Pretty[DynamicSequencingParameters] =
    prettyOfClass(
      paramWithoutValue("payload", _.payload.isDefined)
    )

  def toProtoV30: v30.DynamicSequencingParameters =
    v30.DynamicSequencingParameters(
      payload.fold(ByteString.empty())(identity)
    )
}

object DynamicSequencingParameters
    extends HasProtocolVersionedCompanion[DynamicSequencingParameters] {

  def default(
      representativeProtocolVersion: RepresentativeProtocolVersion[
        DynamicSequencingParameters.type
      ]
  ): DynamicSequencingParameters =
    DynamicSequencingParameters(None)(representativeProtocolVersion)

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v33)(
      v30.DynamicSequencingParameters
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "dynamic sequencing parameters"

  def fromProtoV30(
      sequencingDynamicParameters: v30.DynamicSequencingParameters
  ): ParsingResult[DynamicSequencingParameters] = {
    val payload = sequencingDynamicParameters.payload
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield DynamicSequencingParameters(Option.when(!payload.isEmpty)(payload))(rpv)
  }
}

/** Dynamic sequencing parameters and their validity interval.
  *
  * @param validFrom Start point of the validity interval (exclusive)
  * @param validUntil End point of the validity interval (inclusive)
  */
final case class DynamicSequencingParametersWithValidity(
    parameters: DynamicSequencingParameters,
    validFrom: CantonTimestamp,
    validUntil: Option[CantonTimestamp],
    synchronizerId: SynchronizerId,
) {
  def map[T](f: DynamicSequencingParameters => T): SynchronizerParameters.WithValidity[T] =
    SynchronizerParameters.WithValidity(validFrom, validUntil, f(parameters))

  def isValidAt(ts: CantonTimestamp): Boolean =
    validFrom < ts && validUntil.forall(ts <= _)
}
