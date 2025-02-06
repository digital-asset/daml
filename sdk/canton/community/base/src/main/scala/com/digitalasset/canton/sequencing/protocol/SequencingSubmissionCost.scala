// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

/** Encodes the submission cost calculated by the sender.
  * It will be validated by the sequencer and the submission will be rejected if the cost is incorrect.
  */
final case class SequencingSubmissionCost(
    cost: NonNegativeLong
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencingSubmissionCost.type
    ]
) extends HasProtocolVersionedWrapper[SequencingSubmissionCost]
    with PrettyPrinting {
  @transient override protected lazy val companionObj: SequencingSubmissionCost.type =
    SequencingSubmissionCost

  private[canton] def toProtoV30: v30.SequencingSubmissionCost = v30.SequencingSubmissionCost(
    cost = cost.value
  )

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    param("sequencing submission cost", _.cost)
  )

  def copy(
      cost: NonNegativeLong
  ): SequencingSubmissionCost =
    SequencingSubmissionCost(cost)(representativeProtocolVersion)
}

object SequencingSubmissionCost
    extends VersioningCompanion[SequencingSubmissionCost]
    with ProtocolVersionedCompanionDbHelpers[SequencingSubmissionCost] {

  def apply(
      cost: NonNegativeLong,
      protocolVersion: ProtocolVersion,
  ): SequencingSubmissionCost =
    SequencingSubmissionCost(cost)(protocolVersionRepresentativeFor(protocolVersion))

  override def name: String = "SequencingSubmissionCost"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v33)(v30.SequencingSubmissionCost)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private[canton] def fromProtoV30(
      proto: v30.SequencingSubmissionCost
  ): ParsingResult[SequencingSubmissionCost] = {
    val v30.SequencingSubmissionCost(costP) = proto
    for {
      cost <- ProtoConverter.parseNonNegativeLong("cost", costP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencingSubmissionCost(cost, rpv.representative)
  }
}
