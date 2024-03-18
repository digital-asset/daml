// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v30
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.MemberTrafficSnapshot
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalance
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
    trafficBalances: Seq[TrafficBalance],
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[SequencerSnapshot.type])
    extends HasProtocolVersionedWrapper[SequencerSnapshot] {

  @transient override protected lazy val companionObj: SequencerSnapshot.type = SequencerSnapshot

  def toProtoV30: v30.SequencerSnapshot = {
    def serializeInFlightAggregation(
        args: (AggregationId, InFlightAggregation)
    ): v30.SequencerSnapshot.InFlightAggregationWithId = {
      val (aggregationId, InFlightAggregation(aggregatedSenders, maxSequencingTime, rule)) = args
      v30.SequencerSnapshot.InFlightAggregationWithId(
        aggregationId.toProtoPrimitive,
        Some(rule.toProtoV30),
        maxSequencingTime.toProtoPrimitive,
        aggregatedSenders.toSeq.map {
          case (sender, AggregationBySender(sequencingTimestamp, signatures)) =>
            v30.SequencerSnapshot.AggregationBySender(
              sender.toProtoPrimitive,
              sequencingTimestamp.toProtoPrimitive,
              signatures.map(sigsOnEnv =>
                v30.SequencerSnapshot.SignaturesForEnvelope(sigsOnEnv.map(_.toProtoV30))
              ),
            )
        },
      )
    }

    v30.SequencerSnapshot(
      latestTimestamp = lastTs.toProtoPrimitive,
      headMemberCounters =
        // TODO(#12075) sortBy is a poor man's approach to achieving deterministic serialization here
        //  Figure out whether we need this for sequencer snapshots
        heads.toSeq.sortBy { case (member, _counter) => member }.map { case (member, counter) =>
          v30.SequencerSnapshot.MemberCounter(member.toProtoPrimitive, counter.toProtoPrimitive)
        },
      status = Some(status.toProtoV30),
      inFlightAggregations = inFlightAggregations.toSeq.map(serializeInFlightAggregation),
      additional =
        additional.map(a => v30.ImplementationSpecificInfo(a.implementationName, a.info)),
      trafficSnapshots = trafficSnapshots.toList.map { case (_member, snapshot) =>
        snapshot.toProtoV30
      },
      trafficBalances = trafficBalances.map(_.toProtoV30),
    )
  }
}

object SequencerSnapshot extends HasProtocolVersionedCompanion[SequencerSnapshot] {
  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v30)(v30.SequencerSnapshot)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
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
      trafficBalances: Seq[TrafficBalance],
  ): SequencerSnapshot =
    SequencerSnapshot(
      lastTs,
      heads,
      status,
      inFlightAggregations,
      additional,
      trafficState,
      trafficBalances,
    )(protocolVersionRepresentativeFor(protocolVersion))

  def unimplemented(protocolVersion: ProtocolVersion): SequencerSnapshot = SequencerSnapshot(
    CantonTimestamp.MinValue,
    Map.empty,
    SequencerPruningStatus.Unimplemented,
    Map.empty,
    None,
    Map.empty,
    Seq.empty,
  )(protocolVersionRepresentativeFor(protocolVersion))

  final case class ImplementationSpecificInfo(implementationName: String, info: ByteString)

  def fromProtoV30(
      request: v30.SequencerSnapshot
  ): ParsingResult[SequencerSnapshot] = {
    def parseInFlightAggregationWithId(
        proto: v30.SequencerSnapshot.InFlightAggregationWithId
    ): ParsingResult[(AggregationId, InFlightAggregation)] = {
      val v30.SequencerSnapshot.InFlightAggregationWithId(
        aggregationIdP,
        aggregationRuleP,
        maxSequencingTimeP,
        aggregatedSendersP,
      ) = proto
      for {
        aggregationId <- AggregationId.fromProtoPrimitive(aggregationIdP)
        aggregationRule <- ProtoConverter.parseRequired(
          AggregationRule.fromProtoV30,
          "v30.SequencerSnapshot.InFlightAggregationWithId.aggregation_rule",
          aggregationRuleP,
        )
        maxSequencingTime <- CantonTimestamp.fromProtoPrimitive(maxSequencingTimeP)
        aggregatedSenders <- aggregatedSendersP
          .traverse {
            case v30.SequencerSnapshot.AggregationBySender(
                  senderP,
                  sequencingTimestampP,
                  signaturesByEnvelopeP,
                ) =>
              for {
                sender <- Member.fromProtoPrimitive(
                  senderP,
                  "v30.SequencerSnapshot.AggregationBySender.sender",
                )
                sequencingTimestamp <- CantonTimestamp.fromProtoPrimitive(sequencingTimestampP)
                signatures <- signaturesByEnvelopeP.traverse {
                  case v30.SequencerSnapshot.SignaturesForEnvelope(sigsOnEnv) =>
                    sigsOnEnv.traverse(Signature.fromProtoV30)
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
      lastTs <- CantonTimestamp.fromProtoPrimitive(request.latestTimestamp)
      heads <- request.headMemberCounters
        .traverse { case v30.SequencerSnapshot.MemberCounter(member, counter) =>
          Member
            .fromProtoPrimitive(member, "registeredMembers")
            .map(m => m -> SequencerCounter(counter))
        }
        .map(_.toMap)
      status <- ProtoConverter.parseRequired(
        SequencerPruningStatus.fromProtoV30,
        "status",
        request.status,
      )
      inFlightAggregations <- request.inFlightAggregations
        .traverse(parseInFlightAggregationWithId)
        .map(_.toMap)
      trafficSnapshots <- request.trafficSnapshots.traverse(MemberTrafficSnapshot.fromProtoV30)
      trafficBalances <- request.trafficBalances.traverse(TrafficBalance.fromProtoV30)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerSnapshot(
      lastTs,
      heads,
      status,
      inFlightAggregations,
      request.additional.map(a => ImplementationSpecificInfo(a.implementationName, a.info)),
      trafficSnapshots = trafficSnapshots.map(s => s.member -> s).toMap,
      trafficBalances = trafficBalances,
    )(rpv)
  }
}

final case class SequencerInitialState(
    domainId: DomainId,
    snapshot: SequencerSnapshot,
    // TODO(#13883,#14504) Revisit whether this still makes sense: For sequencer onboarding, this timestamp
    //  will typically differ between sequencers because they may receive envelopes addressed directly to them
    //  even though this should not happen during a normal protocol run.
    latestSequencerEventTimestamp: Option[CantonTimestamp],
    initialTopologyEffectiveTimestamp: Option[CantonTimestamp],
)

object SequencerInitialState {
  def apply(
      domainId: DomainId,
      snapshot: SequencerSnapshot,
      times: SeqView[(CantonTimestamp, CantonTimestamp)],
  ): SequencerInitialState = {
    // TODO(#14504) Update since we now also need to look at top-ups
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
