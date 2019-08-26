// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Duration

import com.digitalasset.daml.lf.data.Ref

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  */
final case class Configuration(
    /* The configuration generation. Monotonically increasing. */
    generation: Long,
    /** The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands. */
    timeModel: TimeModel,
    /** The identity of the participant allowed to change the configuration. If not set, any participant
      * can change the configuration. */
    authorizedParticipantId: Option[ParticipantId],
    /** Flag to enable "open world" mode in which submissions from unallocated parties are allowed through. Useful in testing. */
    openWorld: Boolean
)

object Configuration {
  import com.daml.ledger.participant.state.protobuf

  val protobufVersion: Long = 1L

  def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    config.getVersion match {
      case 1 => DecodeV1.decode(config)
      case v => Left("Unknown version: $v")
    }

  private object DecodeV1 {

    def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
      for {
        tm <- if (config.hasTimeModel)
          decodeTimeModel(config.getTimeModel)
        else
          Left("Missing time model")

        authPidRaw = config.getAuthorizedParticipantId
        authPid <- if (authPidRaw.isEmpty)
          Right(None)
        else
          Ref.LedgerString.fromString(authPidRaw).map(Some(_))
      } yield {
        Configuration(
          generation = config.getGeneration,
          timeModel = tm,
          authorizedParticipantId = authPid,
          openWorld = config.getOpenWorld
        )
      }

    def decodeTimeModel(tm: protobuf.LedgerTimeModel): Either[String, TimeModel] =
      TimeModelImpl(
        maxClockSkew = parseDuration(tm.getMaxClockSkew),
        minTransactionLatency = parseDuration(tm.getMinTransactionLatency),
        maxTtl = parseDuration(tm.getMaxTtl)
      ).toEither.left.map(e => s"decodeTimeModel: ${e.getMessage}")

  }

  def encode(config: Configuration): protobuf.LedgerConfiguration = {
    val tm = config.timeModel
    protobuf.LedgerConfiguration.newBuilder
      .setVersion(protobufVersion)
      .setGeneration(config.generation)
      .setAuthorizedParticipantId(config.authorizedParticipantId.fold("")(identity))
      .setOpenWorld(config.openWorld)
      .setTimeModel(
        protobuf.LedgerTimeModel.newBuilder
          .setMaxClockSkew(buildDuration(tm.maxClockSkew))
          .setMinTransactionLatency(buildDuration(tm.minTransactionLatency))
          .setMaxTtl(buildDuration(tm.maxTtl))
      )
      .build

  }

  // TODO(JM): Find a common place for these.
  private def parseDuration(dur: com.google.protobuf.Duration): Duration = {
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)
  }

  private def buildDuration(dur: Duration): com.google.protobuf.Duration = {
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
  }

}
