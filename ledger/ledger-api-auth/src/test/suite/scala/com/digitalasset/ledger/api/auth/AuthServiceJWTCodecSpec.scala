// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{Matchers, WordSpec}
import spray.json._

import java.time.Instant
import scala.util.{Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class AuthServiceJWTCodecSpec
    extends WordSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {

  /** Serializes a [[AuthServiceJWTPayload]] to JSON, then parses it back to a AuthServiceJWTPayload */
  private def serializeAndParse(value: AuthServiceJWTPayload): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed
  }

  private implicit val arbInstant: Arbitrary[Instant] = {
    Arbitrary {
      for {
        millis <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
        nanos <- Gen.chooseNum(Instant.MIN.getNano, Instant.MAX.getNano)
      } yield {
        Instant.ofEpochMilli(millis).plusNanos(nanos.toLong)
      }
    }
  }

  "AuthServiceJWTPayload codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary values" in forAll(
        Gen.resultOf(AuthServiceJWTPayload),
        minSuccessful(100))(
        value => serializeAndParse(value) shouldBe Success(value)
      )
    }
  }
}
