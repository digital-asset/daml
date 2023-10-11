// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.{v0, v1}
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.MemberTrafficSnapshot
import com.digitalasset.canton.sequencing.protocol.{AggregationId, AggregationRule}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter}
import com.google.protobuf.ByteString

import scala.collection.SeqView

final case class SequencerSnapshot(
    lastTs: CantonTimestamp,
    heads: Map[Member, SequencerCounter],
    status: SequencerPruningStatus,
    inFlightAggregations: InFlightAggregations,
    additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
    trafficSnapshots: Map[Member, MemberTrafficSnapshot],
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

  def toProtoV1: v1.SequencerSnapshot = {
    def serializeInFlightAggregation(
        args: (AggregationId, InFlightAggregation)
    ): v1.SequencerSnapshot.InFlightAggregationWithId = {
      val (aggregationId, InFlightAggregation(aggregatedSenders, maxSequencingTime, rule)) = args
      v1.SequencerSnapshot.InFlightAggregationWithId(
        aggregationId.toProtoPrimitive,
        Some(rule.toProtoV0),
        Some(maxSequencingTime.toProtoPrimitive),
        aggregatedSenders.toSeq.map {
          case (sender, AggregationBySender(sequencingTimestamp, signatures)) =>
            v1.SequencerSnapshot.AggregationBySender(
              sender.toProtoPrimitive,
              Some(sequencingTimestamp.toProtoPrimitive),
              signatures.map(sigsOnEnv =>
                v1.SequencerSnapshot.SignaturesForEnvelope(sigsOnEnv.map(_.toProtoV0))
              ),
            )
        },
      )
    }

    v1.SequencerSnapshot(
      latestTimestamp = Some(lastTs.toProtoPrimitive),
      headMemberCounters =
        // TODO(#12075) sortBy is a poor man's approach to achieving deterministic serialization here
        //  Figure out whether we need this for sequencer snapshots
        heads.toSeq.sortBy { case (member, _counter) => member }.map { case (member, counter) =>
          v1.SequencerSnapshot.MemberCounter(member.toProtoPrimitive, counter.toProtoPrimitive)
        },
      status = Some(status.toProtoV0),
      inFlightAggregations = inFlightAggregations.toSeq.map(serializeInFlightAggregation),
      additional = additional.map(a => v0.ImplementationSpecificInfo(a.implementationName, a.info)),
      trafficSnapshots = trafficSnapshots.toList.map { case (member, snapshot) =>
        snapshot.toProtoV1
      },
    )
  }
}

object SequencerSnapshot extends HasProtocolVersionedCompanion[SequencerSnapshot] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(v0.SequencerSnapshot)(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(ProtocolVersion.CNTestNet)(v1.SequencerSnapshot)(
      supportedProtoVersion(_)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  override def name: String = "sequencer snapshot"

  def apply(
      lastTs: CantonTimestamp,
      heads: Map[Member, SequencerCounter],
      status: SequencerPruningStatus,
      inFlightAggregations: InFlightAggregations,
      additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
      protocolVersion: ProtocolVersion,
      trafficState: Map[Member, MemberTrafficSnapshot],
  ): SequencerSnapshot =
    SequencerSnapshot(lastTs, heads, status, inFlightAggregations, additional, trafficState)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def unimplemented(protocolVersion: ProtocolVersion) = SequencerSnapshot(
    CantonTimestamp.MinValue,
    Map.empty,
    SequencerPruningStatus.Unimplemented,
    Map.empty,
    None,
    Map.empty,
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
    } yield SequencerSnapshot(
      lastTs,
      heads,
      status,
      Map.empty,
      request.additional.map(a => ImplementationSpecificInfo(a.implementationName, a.info)),
      Map.empty,
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))

  def fromProtoV1(
      request: v1.SequencerSnapshot
  ): ParsingResult[SequencerSnapshot] = {
    def parseInFlightAggregationWithId(
        proto: v1.SequencerSnapshot.InFlightAggregationWithId
    ): ParsingResult[(AggregationId, InFlightAggregation)] = {
      val v1.SequencerSnapshot.InFlightAggregationWithId(
        aggregationIdP,
        aggregationRuleP,
        maxSequencingTimeP,
        aggregatedSendersP,
      ) = proto
      for {
        aggregationId <- AggregationId.fromProtoPrimitive(aggregationIdP)
        aggregationRule <- ProtoConverter.parseRequired(
          AggregationRule.fromProtoV0,
          "v1.SequencerSnapshot.InFlightAggregationWithId.aggregation_rule",
          aggregationRuleP,
        )
        maxSequencingTime <- ProtoConverter.parseRequired(
          CantonTimestamp.fromProtoPrimitive,
          "v1.SequencerSnapshot.InFlightAggregationWithId.max_sequencing_time",
          maxSequencingTimeP,
        )
        aggregatedSenders <- aggregatedSendersP
          .traverse {
            case v1.SequencerSnapshot.AggregationBySender(
                  senderP,
                  sequencingTimestampP,
                  signaturesByEnvelopeP,
                ) =>
              for {
                sender <- Member.fromProtoPrimitive(
                  senderP,
                  "v1.SequencerSnapshot.AggregationBySender.sender",
                )
                sequencingTimestamp <- ProtoConverter.parseRequired(
                  CantonTimestamp.fromProtoPrimitive,
                  "v1.SequencerSnapshot.AggregationBySender.sequencing_timestamp",
                  sequencingTimestampP,
                )
                signatures <- signaturesByEnvelopeP.traverse {
                  case v1.SequencerSnapshot.SignaturesForEnvelope(sigsOnEnv) =>
                    sigsOnEnv.traverse(Signature.fromProtoV0)
                }
              } yield sender -> AggregationBySender(sequencingTimestamp, signatures)
          }
          .map(_.toMap)
        inFlightAggregation <- InFlightAggregation
          .create(
            aggregatedSenders,
            maxSequencingTime,
            aggregationRule,
          )
          .leftMap(err => ProtoDeserializationError.InvariantViolation(err))
      } yield aggregationId -> inFlightAggregation
    }

    for {
      lastTs <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "latestTimestamp",
        request.latestTimestamp,
      )
      heads <- request.headMemberCounters
        .traverse { case v1.SequencerSnapshot.MemberCounter(member, counter) =>
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
      inFlightAggregations <- request.inFlightAggregations
        .traverse(parseInFlightAggregationWithId)
        .map(_.toMap)
      trafficSnapshots <- request.trafficSnapshots.traverse(MemberTrafficSnapshot.fromProtoV1)
    } yield SequencerSnapshot(
      lastTs,
      heads,
      status,
      inFlightAggregations,
      request.additional.map(a => ImplementationSpecificInfo(a.implementationName, a.info)),
      trafficSnapshots = trafficSnapshots.map(s => s.member -> s).toMap,
    )(protocolVersionRepresentativeFor(ProtoVersion(1)))
  }
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
