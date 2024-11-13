// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.pruning

import com.digitalasset.canton.admin.participant.v30 as partV30
import com.digitalasset.canton.admin.pruning.v30 as prunV30
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseNonNegativeLong}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.*

final case class ConfigForDomainThresholds(
    domainId: DomainId,
    thresholdDistinguished: NonNegativeLong,
    thresholdDefault: NonNegativeLong,
)

object ConfigForDomainThresholds {

  def fromProto30(
      config: partV30.SlowCounterParticipantDomainConfig
  ): Seq[ParsingResult[ConfigForDomainThresholds]] =
    for {
      domainIdAsString <- config.domainIds
    } yield {
      for {
        domainId <- DomainId.fromProtoPrimitive(domainIdAsString, "domainId")
        thresholdDistinguished <- parseNonNegativeLong(
          "thresholdDistinguished",
          config.thresholdDistinguished,
        )
        thresholdDefault <- parseNonNegativeLong("thresholdDefault", config.thresholdDefault)
      } yield ConfigForDomainThresholds(
        domainId,
        thresholdDistinguished,
        thresholdDefault,
      )
    }
}

final case class ConfigForSlowCounterParticipants(
    domainId: DomainId,
    participantId: ParticipantId,
    isDistinguished: Boolean,
    isAddedToMetrics: Boolean,
) {

  def toProto30(
      thresholds: ConfigForDomainThresholds
  ): partV30.SlowCounterParticipantDomainConfig =
    partV30.SlowCounterParticipantDomainConfig(
      domainIds = Seq(domainId.toProtoPrimitive),
      distinguishedParticipantUids =
        if (isDistinguished) Seq(participantId.toProtoPrimitive) else Seq.empty,
      thresholds.thresholdDistinguished.value,
      thresholds.thresholdDefault.value,
      participantUidsMetrics =
        if (isAddedToMetrics) Seq(participantId.toProtoPrimitive) else Seq.empty,
    )

}

object ConfigForSlowCounterParticipants {

  def fromProto30(
      config: partV30.SlowCounterParticipantDomainConfig
  ): Seq[ParsingResult[ConfigForSlowCounterParticipants]] =
    for {
      domainIdAsString <- config.domainIds
      participantIdAsString <-
        config.distinguishedParticipantUids ++ config.participantUidsMetrics
    } yield {
      for {
        domainId <- DomainId.fromProtoPrimitive(domainIdAsString, "domainId")
        participantId <- ParticipantId
          .fromProtoPrimitive(participantIdAsString, "counter_participant_uid")
      } yield ConfigForSlowCounterParticipants(
        domainId,
        participantId,
        config.distinguishedParticipantUids.contains(participantIdAsString),
        config.participantUidsMetrics.contains(participantIdAsString),
      )
    }
}

final case class ConfigForNoWaitCounterParticipants(
    domainId: DomainId,
    participantId: ParticipantId,
) {

  def toProtoV30: prunV30.WaitCommitmentsSetup =
    prunV30.WaitCommitmentsSetup(
      participantId.toProtoPrimitive,
      Some(prunV30.Domains(Seq(domainId.toProtoPrimitive))),
    )

}

final case class CounterParticipantIntervalsBehind(
    domainId: DomainId,
    participantId: ParticipantId,
    intervalsBehind: NonNegativeLong,
    timeBehind: NonNegativeFiniteDuration,
    asOfSequencingTime: CantonTimestamp,
) extends HasVersionedWrapper[CounterParticipantIntervalsBehind]
    with PrettyPrinting {
  def toProtoV30: partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo =
    partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo(
      participantId.toProtoPrimitive,
      domainId.toProtoPrimitive,
      intervalsBehind.value,
      Some(timeBehind.toProtoPrimitive),
      Some(asOfSequencingTime.toProtoTimestamp),
    )

  override protected def companionObj = CounterParticipantIntervalsBehind

  override def pretty: Pretty[CounterParticipantIntervalsBehind] =
    prettyOfClass(
      param("domain", _.domainId),
      param("participant", _.participantId),
      param("intervals behind", _.intervalsBehind),
      param("time behind", _.timeBehind),
      param("sequencing timestamp", _.asOfSequencingTime),
    )
}

object CounterParticipantIntervalsBehind
    extends HasVersionedMessageCompanion[CounterParticipantIntervalsBehind]
    with HasVersionedMessageCompanionDbHelpers[CounterParticipantIntervalsBehind] {

  override def name: String = "counter participant intervals behind"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(
        partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo
      )(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[pruning] def fromProtoV30(
      counterParticipantInfoProto: partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo
  ): ParsingResult[CounterParticipantIntervalsBehind] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(
        counterParticipantInfoProto.counterParticipantUid,
        "counterParticipantUid",
      )
      domainId <- DomainId.fromProtoPrimitive(counterParticipantInfoProto.domainId, "domainId")
      intervalsBehind <- parseNonNegativeLong(
        "thresholdDistinguished",
        counterParticipantInfoProto.intervalsBehind,
      )
      timeBehind <- NonNegativeFiniteDuration.fromProtoPrimitiveO("timeBehind")(
        counterParticipantInfoProto.behindSince
      )
      asOfSequencingTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "asOfSequencingTime",
        counterParticipantInfoProto.asOfSequencingTimestamp,
      )
    } yield CounterParticipantIntervalsBehind(
      domainId,
      participantId,
      intervalsBehind,
      timeBehind,
      asOfSequencingTime,
    )

}
