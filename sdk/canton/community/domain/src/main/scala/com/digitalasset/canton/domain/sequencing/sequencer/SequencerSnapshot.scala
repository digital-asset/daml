// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.traverse.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import scala.collection.SeqView

final case class SequencerSnapshot(
    lastTs: CantonTimestamp,
    heads: Map[Member, SequencerCounter],
    status: SequencerPruningStatus,
    additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencerSnapshot.type])
    extends HasProtocolVersionedWrapper[SequencerSnapshot] {

  @transient override protected lazy val companionObj: SequencerSnapshot.type = SequencerSnapshot

  def toProtoV0: v0.SequencerSnapshot = v0.SequencerSnapshot(
    Some(lastTs.toProtoPrimitive),
    heads.map { case (member, counter) =>
      member.toProtoPrimitive -> counter.toProtoPrimitive
    },
    Some(status.toProtoV0),
    additional.map(a => v0.ImplementationSpecificInfo(a.implementationName, a.info)),
  )
}

object SequencerSnapshot extends HasProtocolVersionedCompanion[SequencerSnapshot] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.SequencerSnapshot)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "sequencer snapshot"

  def apply(
      lastTs: CantonTimestamp,
      heads: Map[Member, SequencerCounter],
      status: SequencerPruningStatus,
      additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
      protocolVersion: ProtocolVersion,
  ): SequencerSnapshot =
    SequencerSnapshot(lastTs, heads, status, additional)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def unimplemented(protocolVersion: ProtocolVersion) = SequencerSnapshot(
    CantonTimestamp.MinValue,
    Map.empty,
    SequencerPruningStatus.Unimplemented,
    None,
  )(protocolVersionRepresentativeFor(protocolVersion))

  final case class ImplementationSpecificInfo(implementationName: String, info: ByteString)

  def fromProtoV0(
      request: v0.SequencerSnapshot
  ): ParsingResult[SequencerSnapshot] =
    for {
      lastTs <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "latestTimestamp",
        request.latestTimestamp,
      )
      heads <- request.headMemberCounters.toList
        .traverse { case (member, counter) =>
          Member
            .fromProtoPrimitive(member, "registeredMembers")
            .map(m => m -> SequencerCounter(counter))
        }
        .map(_.toMap)
      status <- ProtoConverter.parseRequired(
        SequencerPruningStatus.fromProtoV0,
        "status",
        request.status,
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(0))
    } yield SequencerSnapshot(
      lastTs,
      heads,
      status,
      request.additional.map(a => ImplementationSpecificInfo(a.implementationName, a.info)),
    )(rpv)
}

final case class SequencerInitialState(
    domainId: DomainId,
    snapshot: SequencerSnapshot,
    latestTopologyClientTimestamp: Option[CantonTimestamp],
    initialTopologyEffectiveTimestamp: Option[CantonTimestamp],
)

object SequencerInitialState {
  def apply(
      domainId: DomainId,
      snapshot: SequencerSnapshot,
      times: SeqView[(CantonTimestamp, CantonTimestamp)],
  ): SequencerInitialState = {
    /* Take the sequencing time of the last topology update for the latest topology client timestamp.
     * There may have been further events addressed to the topology client member after this topology update,
     * but it is safe to ignore them. We assume that the topology snapshot includes all changes that have
     * been sequenced up to the sequencer snapshot.
     *
     * Analogously take the maximum effective time mark the initial topology snapshot effective time.
     * The sequencer topology snapshot can include events that become effective after the sequencer's onboarding
     * transaction, and as a result "effectiveTimes.maxOption" can be higher than strictly necessary to prevent
     * tombstoned signing failures, but at least the additional time is bounded by the topology change delay.
     */
    val (sequencedTimes, effectiveTimes) = times.unzip
    SequencerInitialState(domainId, snapshot, sequencedTimes.maxOption, effectiveTimes.maxOption)
  }
}
