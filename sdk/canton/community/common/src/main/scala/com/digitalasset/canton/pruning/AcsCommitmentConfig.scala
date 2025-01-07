// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.version.*

final case class ConfigForDomainThresholds(
    synchronizerId: SynchronizerId,
    thresholdDistinguished: NonNegativeLong,
    thresholdDefault: NonNegativeLong,
)

object ConfigForDomainThresholds {

  def fromProto30(
      config: partV30.SlowCounterParticipantDomainConfig
  ): Seq[ParsingResult[ConfigForDomainThresholds]] =
    for {
      synchronizerIdAsString <- config.synchronizerIds
    } yield {
      for {
        synchronizerId <- SynchronizerId.fromProtoPrimitive(
          synchronizerIdAsString,
          "synchronizerId",
        )
        thresholdDistinguished <- parseNonNegativeLong(
          "thresholdDistinguished",
          config.thresholdDistinguished,
        )
        thresholdDefault <- parseNonNegativeLong("thresholdDefault", config.thresholdDefault)
      } yield ConfigForDomainThresholds(
        synchronizerId,
        thresholdDistinguished,
        thresholdDefault,
      )
    }
}

final case class ConfigForSlowCounterParticipants(
    synchronizerId: SynchronizerId,
    participantId: ParticipantId,
    isDistinguished: Boolean,
    isAddedToMetrics: Boolean,
) {

  def toProto30(
      thresholds: ConfigForDomainThresholds
  ): partV30.SlowCounterParticipantDomainConfig =
    partV30.SlowCounterParticipantDomainConfig(
      synchronizerIds = Seq(synchronizerId.toProtoPrimitive),
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
      synchronizerIdAsString <- config.synchronizerIds
      participantIdAsString <-
        config.distinguishedParticipantUids ++ config.participantUidsMetrics
    } yield {
      for {
        synchronizerId <- SynchronizerId.fromProtoPrimitive(
          synchronizerIdAsString,
          "synchronizerId",
        )
        participantId <- ParticipantId
          .fromProtoPrimitive(participantIdAsString, "counter_participant_uid")
      } yield ConfigForSlowCounterParticipants(
        synchronizerId,
        participantId,
        config.distinguishedParticipantUids.contains(participantIdAsString),
        config.participantUidsMetrics.contains(participantIdAsString),
      )
    }
}

final case class ConfigForNoWaitCounterParticipants(
    synchronizerId: SynchronizerId,
    participantId: ParticipantId,
) {

  def toProtoV30: prunV30.WaitCommitmentsSetup =
    prunV30.WaitCommitmentsSetup(
      participantId.toProtoPrimitive,
      Some(prunV30.Domains(Seq(synchronizerId.toProtoPrimitive))),
    )

}

final case class CounterParticipantIntervalsBehind(
    synchronizerId: SynchronizerId,
    participantId: ParticipantId,
    intervalsBehind: NonNegativeLong,
    timeBehind: NonNegativeFiniteDuration,
    asOfSequencingTime: CantonTimestamp,
) extends HasVersionedWrapper[CounterParticipantIntervalsBehind]
    with PrettyPrinting {
  def toProtoV30: partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo =
    partV30.GetIntervalsBehindForCounterParticipants.CounterParticipantInfo(
      participantId.toProtoPrimitive,
      synchronizerId.toProtoPrimitive,
      intervalsBehind.value,
      Some(timeBehind.toProtoPrimitive),
      Some(asOfSequencingTime.toProtoTimestamp),
    )

  override protected def companionObj = CounterParticipantIntervalsBehind

  override def pretty: Pretty[CounterParticipantIntervalsBehind] =
    prettyOfClass(
      param("domain", _.synchronizerId),
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
      _.toProtoV30,
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
      synchronizerId <- SynchronizerId.fromProtoPrimitive(
        counterParticipantInfoProto.synchronizerId,
        "synchronizerId",
      )
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
      synchronizerId,
      participantId,
      intervalsBehind,
      timeBehind,
      asOfSequencingTime,
    )

}
