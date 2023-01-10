// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.configuration

import java.time.Duration

import scala.util.Try

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  *
  * @param generation                The configuration generation. Monotonically increasing.
  * @param timeModel                 The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands.
  * @param maxDeduplicationDuration  The maximum time window during which commands can be deduplicated.
  */
final case class Configuration(
    generation: Long,
    timeModel: LedgerTimeModel,
    maxDeduplicationDuration: Duration,
)

object Configuration {

  /** The first configuration generation, by convention. */
  val StartingGeneration = 1L

  /** Version history:
    * V1: initial version
    * V2: added maxDeduplicationTime
    */
  val protobufVersion = 2L

  /** A duration of 1 day is likely to be longer than any application will keep retrying, barring
    * very strange events, and should work for most ledger and participant configurations.
    */
  val reasonableMaxDeduplicationDuration: Duration = Duration.ofDays(1)

  val reasonableInitialConfiguration: Configuration = Configuration(
    generation = StartingGeneration,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationDuration = reasonableMaxDeduplicationDuration,
  )

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
      .setMaxDeduplicationDuration(buildDuration(config.maxDeduplicationDuration))
      .build
  }

  def decode(bytes: Array[Byte]): Either[String, Configuration] =
    Try(protobuf.LedgerConfiguration.parseFrom(bytes)).toEither.left
      .map(_.getMessage)
      .flatMap(decode)

  def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    config.getVersion match {
      case 1 => decodeV1(config)
      case 2 => decodeV2(config)
      case v => Left(s"Unknown version: $v")
    }

  private def decodeV1(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    for {
      tm <-
        if (config.hasTimeModel) {
          decodeTimeModel(config.getTimeModel)
        } else {
          Left("Missing time model")
        }
    } yield {
      Configuration(
        generation = config.getGeneration,
        timeModel = tm,
        maxDeduplicationDuration = Duration.ofDays(1),
      )
    }

  private def decodeV2(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    for {
      tm <-
        if (config.hasTimeModel) {
          decodeTimeModel(config.getTimeModel)
        } else {
          Left("Missing time model")
        }
      maxDeduplicationTime <-
        if (config.hasMaxDeduplicationDuration) {
          val duration = parseDuration(config.getMaxDeduplicationDuration)
          if (duration.isNegative) {
            Left("requirement failed: Negative maximum command time to live")
          } else {
            Right(duration)
          }
        } else {
          Left("Missing maximum command time to live")
        }
    } yield {
      Configuration(
        generation = config.getGeneration,
        timeModel = tm,
        maxDeduplicationDuration = maxDeduplicationTime,
      )
    }

  private def decodeTimeModel(tm: protobuf.LedgerTimeModel): Either[String, LedgerTimeModel] =
    LedgerTimeModel(
      avgTransactionLatency = parseDuration(tm.getAvgTransactionLatency),
      minSkew = parseDuration(tm.getMinSkew),
      maxSkew = parseDuration(tm.getMaxSkew),
    ).toEither.left.map(e => s"decodeTimeModel: ${e.getMessage}")

  private def parseDuration(dur: com.google.protobuf.Duration): Duration =
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)

  private def buildDuration(dur: Duration): com.google.protobuf.Duration =
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
}
