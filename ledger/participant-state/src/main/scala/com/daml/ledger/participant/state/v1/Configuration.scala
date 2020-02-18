// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Duration

import scala.util.Try

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  */
final case class Configuration(
    /* The configuration generation. Monotonically increasing. */
    generation: Long,
    /** The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands. */
    timeModel: TimeModel,
)

object Configuration {
  import com.daml.ledger.participant.state.protobuf

  val protobufVersion: Long = 1L

  def decode(bytes: Array[Byte]): Either[String, Configuration] =
    Try(protobuf.LedgerConfiguration.parseFrom(bytes)).toEither.left
      .map(_.getMessage)
      .right
      .flatMap(decode)

  def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    config.getVersion match {
      case 1 => DecodeV1.decode(config)
      case v => Left(s"Unknown version: $v")
    }

  private object DecodeV1 {

    def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
      for {
        tm <- if (config.hasTimeModel)
          decodeTimeModel(config.getTimeModel)
        else
          Left("Missing time model")
      } yield {
        Configuration(
          generation = config.getGeneration,
          timeModel = tm,
        )
      }

    def decodeTimeModel(tm: protobuf.LedgerTimeModel): Either[String, TimeModel] =
      TimeModel(
        maxClockSkew = parseDuration(tm.getMaxClockSkew),
        minTransactionLatency = parseDuration(tm.getMinTransactionLatency),
        maxTtl = parseDuration(tm.getMaxTtl),
        avgTransactionLatency = parseDuration(tm.getAvgTransactionLatency),
        minSkew = parseDuration(tm.getMinSkew),
        maxSkew = parseDuration(tm.getMaxSkew),
      ).toEither.left.map(e => s"decodeTimeModel: ${e.getMessage}")
  }

  def encode(config: Configuration): protobuf.LedgerConfiguration = {
    val tm = config.timeModel
    protobuf.LedgerConfiguration.newBuilder
      .setVersion(protobufVersion)
      .setGeneration(config.generation)
      .setTimeModel(
        protobuf.LedgerTimeModel.newBuilder
          .setMaxClockSkew(buildDuration(tm.maxClockSkew))
          .setMinTransactionLatency(buildDuration(tm.minTransactionLatency))
          .setMaxTtl(buildDuration(tm.maxTtl))
          .setAvgTransactionLatency(buildDuration(tm.avgTransactionLatency))
          .setMinSkew(buildDuration(tm.minSkew))
          .setMaxSkew(buildDuration(tm.maxSkew))
      )
      .build
  }

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
