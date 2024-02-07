// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.configuration

import com.digitalasset.canton.ledger.configuration.ConfigurationSpec.*
import com.digitalasset.canton.ledger.configuration.v2.ledger_configuration as proto
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.*
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

class ConfigurationSpec extends AnyWordSpec with Matchers {
  "a ledger configuration" when {
    "decoding a v1 protobuf" should {
      "decode a valid protobuf" in {
        val configurationBytes = proto.LedgerConfiguration
          .of(
            version = 1,
            generation = 7,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(1.minute.toProtobuf),
                minSkew = Some(30.seconds.toProtobuf),
                maxSkew = Some(2.minutes.toProtobuf),
              )
            ),
            maxDeduplicationDuration = None,
          )
          .toByteArray

        val configuration = Configuration.decode(configurationBytes)

        configuration should be(
          Right(
            Configuration(
              generation = 7,
              timeModel = LedgerTimeModel(
                avgTransactionLatency = 1.minute.toJava,
                minSkew = 30.seconds.toJava,
                maxSkew = 2.minutes.toJava,
              ).get,
              maxDeduplicationDuration = 1.day.toJava,
            )
          )
        )
      }

      "reject a missing time model" in {
        val configurationBytes = proto.LedgerConfiguration
          .of(
            version = 1,
            generation = 2,
            timeModel = None,
            maxDeduplicationDuration = None,
          )
          .toByteArray

        val configuration = Configuration.decode(configurationBytes)

        configuration should be(Left("Missing time model"))
      }
    }

    "decoding a v2 protobuf" should {
      "decode a valid protobuf" in {
        val configurationBytes = proto.LedgerConfiguration
          .of(
            version = 2,
            generation = 3,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(30.seconds.toProtobuf),
                minSkew = Some(20.seconds.toProtobuf),
                maxSkew = Some(5.minutes.toProtobuf),
              )
            ),
            maxDeduplicationDuration = Some(6.hours.toProtobuf),
          )
          .toByteArray

        val configuration = Configuration.decode(configurationBytes)

        configuration should be(
          Right(
            Configuration(
              generation = 3,
              timeModel = LedgerTimeModel(
                avgTransactionLatency = 30.seconds.toJava,
                minSkew = 20.seconds.toJava,
                maxSkew = 5.minutes.toJava,
              ).get,
              maxDeduplicationDuration = 6.hours.toJava,
            )
          )
        )
      }

      val rejections = Table(
        ("error message", "protobuf"),
        (
          "Missing time model",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 4,
            timeModel = None,
            maxDeduplicationDuration = Some(1.day.toProtobuf),
          ),
        ),
        (
          "Missing maximum command time to live",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 1,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(com.google.protobuf.duration.Duration.defaultInstance),
                minSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
                maxSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
              )
            ),
            maxDeduplicationDuration = None,
          ),
        ),
        (
          "decodeTimeModel: requirement failed: Negative average transaction latency",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 1,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some((-5).seconds.toProtobuf),
                minSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
                maxSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
              )
            ),
            maxDeduplicationDuration = Some(com.google.protobuf.duration.Duration.defaultInstance),
          ),
        ),
        (
          "decodeTimeModel: requirement failed: Negative min skew",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 1,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(com.google.protobuf.duration.Duration.defaultInstance),
                minSkew = Some((-30).seconds.toProtobuf),
                maxSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
              )
            ),
            maxDeduplicationDuration = Some(com.google.protobuf.duration.Duration.defaultInstance),
          ),
        ),
        (
          "decodeTimeModel: requirement failed: Negative max skew",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 1,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(com.google.protobuf.duration.Duration.defaultInstance),
                minSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
                maxSkew = Some((-10).seconds.toProtobuf),
              )
            ),
            maxDeduplicationDuration = Some(com.google.protobuf.duration.Duration.defaultInstance),
          ),
        ),
        (
          "requirement failed: Negative maximum command time to live",
          proto.LedgerConfiguration.of(
            version = 2,
            generation = 1,
            timeModel = Some(
              proto.LedgerTimeModel.of(
                avgTransactionLatency = Some(com.google.protobuf.duration.Duration.defaultInstance),
                minSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
                maxSkew = Some(com.google.protobuf.duration.Duration.defaultInstance),
              )
            ),
            maxDeduplicationDuration = Some((-1).day.toProtobuf),
          ),
        ),
      )

      "reject an invalid protobuf" in {
        forAll(rejections) { (errorMessage, protobuf) =>
          val configurationBytes = protobuf.toByteArray

          val configuration = Configuration.decode(configurationBytes)

          configuration should be(Left(errorMessage))
        }
      }
    }

    "decoding a protobuf with an invalid version" should {
      "reject the protobuf" in {
        val configurationBytes = proto.LedgerConfiguration
          .of(
            version = 3,
            generation = 0,
            timeModel = None,
            maxDeduplicationDuration = None,
          )
          .toByteArray

        val configuration = Configuration.decode(configurationBytes)

        configuration should be(Left("Unknown version: 3"))
      }
    }
  }
}

object ConfigurationSpec {
  implicit class Converter(duration: FiniteDuration) {
    def toProtobuf: com.google.protobuf.duration.Duration = {
      val javaDuration = duration.toJava
      new com.google.protobuf.duration.Duration(javaDuration.getSeconds, javaDuration.getNano)
    }
  }
}
