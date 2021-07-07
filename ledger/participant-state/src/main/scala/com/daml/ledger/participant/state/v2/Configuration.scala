// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Duration

import scala.util.Try

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v2.Update.ConfigurationChanged]].
  *
  * @param generation The configuration generation. Monotonically increasing.
  * @param timeModel The time model of the ledger.
  * @param maxDeduplicationTime The WriteService promises to not reject submissions whose deduplication period
  *                             extends for less than or equal to `maxDeduplicationTime` with the reason that
  *                             the deduplication period is too long.
  */
final case class Configuration(
    generation: Long,
    timeModel: TimeModel,
    maxDeduplicationTime: Duration,
)

// TODO(v2) Serialization and deserialization hasn't been adapted yet

object Configuration {
  import com.daml.ledger.participant.state.protobuf

  /** Version history:
    * V1: initial version
    * V2: added maxDeduplicationTime
    */
  val protobufVersion: Long = 2L

  def decode(bytes: Array[Byte]): Either[String, Configuration] =
    Try(protobuf.LedgerConfiguration.parseFrom(bytes)).toEither.left
      .map(_.getMessage)
      .flatMap(decode)

  def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    config.getVersion match {
      case 1 => DecodeV1.decode(config)
      case 2 => DecodeV2.decode(config)
      case v => Left(s"Unknown version: $v")
    }

  private object DecodeV1 {

    def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
      for {
        tm <-
          if (config.hasTimeModel)
            decodeTimeModel(config.getTimeModel)
          else
            Left("Missing time model")
      } yield {
        Configuration(
          generation = config.getGeneration,
          timeModel = tm,
          maxDeduplicationTime = Duration.ofDays(1),
        )
      }

    def decodeTimeModel(tm: protobuf.LedgerTimeModel): Either[String, TimeModel] =
      TimeModel(
        avgTransactionLatency = parseDuration(tm.getAvgTransactionLatency),
        minSkew = parseDuration(tm.getMinSkew),
        maxSkew = parseDuration(tm.getMaxSkew),
      ).toEither.left.map(e => s"decodeTimeModel: ${e.getMessage}")
  }

  private object DecodeV2 {

    def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
      for {
        tm <-
          if (config.hasTimeModel)
            decodeTimeModel(config.getTimeModel)
          else
            Left("Missing time model")
        maxDeduplicationTime <-
          if (config.hasMaxDeduplicationTime)
            Right(parseDuration(config.getMaxDeduplicationTime))
          else
            Left("Missing maximum command time to live")
      } yield {
        Configuration(
          generation = config.getGeneration,
          timeModel = tm,
          maxDeduplicationTime = maxDeduplicationTime,
        )
      }

    def decodeTimeModel(tm: protobuf.LedgerTimeModel): Either[String, TimeModel] =
      TimeModel(
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
          .setAvgTransactionLatency(buildDuration(tm.avgTransactionLatency))
          .setMinSkew(buildDuration(tm.minSkew))
          .setMaxSkew(buildDuration(tm.maxSkew))
      )
      .setMaxDeduplicationTime(buildDuration(config.maxDeduplicationTime))
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
