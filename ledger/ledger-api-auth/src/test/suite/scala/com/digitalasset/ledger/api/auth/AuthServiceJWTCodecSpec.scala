// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth

import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.time.Instant
import scala.util.{Success, Try}

class AuthServiceJWTCodecSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  /** Serializes a [[AuthServiceJWTPayload]] to JSON, then parses it back to a AuthServiceJWTPayload */
  private def serializeAndParse(value: SupportedJWTPayload): Try[SupportedJWTPayload] = {
    import SupportedJWTCodec.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[SupportedJWTPayload])
    } yield parsed
  }

  /** Parses a string as a [[CustomDamlJWTPayload]] */
  private def parseCustomToken(serialized: String): Try[CustomDamlJWTPayload] = {
    import SupportedJWTCodec.JsonImplicits._

    for {
      json <- Try(serialized.parseJson)
      // FIXME: understand how to avoid unwrapping and re-wrapping the `parsed` here
      CustomDamlJWTPayload(parsed) <- Try(json.convertTo[SupportedJWTPayload])
    } yield CustomDamlJWTPayload(parsed)
  }

  /** Parses a [[SupportedJWTPayload]] */
  private def parse(serialized: String): Try[SupportedJWTPayload] = {
    import SupportedJWTCodec.JsonImplicits._

    for {
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[SupportedJWTPayload])
    } yield parsed
  }

  private implicit val arbInstant: Arbitrary[Instant] = {
    Arbitrary {
      for {
        seconds <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
      } yield {
        Instant.ofEpochSecond(seconds)
      }
    }
  }

  "AuthServiceJWTPayload codec" when {

    "serializing and parsing a value" should {

      "work for arbitrary custom Daml token values" in forAll(
        Gen.resultOf(AuthServiceJWTPayload),
        minSuccessful(100),
      )(v0 => {
        val value = CustomDamlJWTPayload(v0)
        serializeAndParse(value) shouldBe Success(value)
      })

      "work for arbitrary standard Daml token values" in forAll(
        Gen.resultOf(AuthServiceJWTPayload),
        minSuccessful(100),
      )(v0 => {
        val value = StandardJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = None,
            participantId = v0.participantId,
            applicationId = Some(v0.applicationId.getOrElse("default-user")),
            exp = v0.exp,
            admin = false,
            actAs = List.empty,
            readAs = List.empty,
          )
        )
        serializeAndParse(value) shouldBe Success(value)
      })

      "support OIDC compliant sandbox format" in {
        val serialized =
          """{
            |  "https://daml.com/ledger-api": {
            |    "ledgerId": "someLedgerId",
            |    "participantId": "someParticipantId",
            |    "applicationId": "someApplicationId",
            |    "admin": true,
            |    "actAs": ["Alice"],
            |    "readAs": ["Alice", "Bob"]
            |  },
            |  "exp": 0
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = Some("someLedgerId"),
            participantId = Some("someParticipantId"),
            applicationId = Some("someApplicationId"),
            exp = Some(Instant.EPOCH),
            admin = true,
            actAs = List("Alice"),
            readAs = List("Alice", "Bob"),
          )
        )
        val result = parseCustomToken(serialized)
        result shouldBe Success(expected)
        result.map(_.payload.party) shouldBe Success(None)
      }

      "support legacy sandbox format" in {
        val serialized =
          """{
            |  "ledgerId": "someLedgerId",
            |  "participantId": "someParticipantId",
            |  "applicationId": "someApplicationId",
            |  "exp": 0,
            |  "admin": true,
            |  "actAs": ["Alice"],
            |  "readAs": ["Alice", "Bob"]
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = Some("someLedgerId"),
            participantId = Some("someParticipantId"),
            applicationId = Some("someApplicationId"),
            exp = Some(Instant.EPOCH),
            admin = true,
            actAs = List("Alice"),
            readAs = List("Alice", "Bob"),
          )
        )
        val result = parseCustomToken(serialized)
        result shouldBe Success(expected)
        result.map(_.payload.party) shouldBe Success(None)
      }

      "support legacy JSON API format" in {
        val serialized =
          """{
            |  "ledgerId": "someLedgerId",
            |  "applicationId": "someApplicationId",
            |  "party": "Alice"
            |}
          """.stripMargin
        val expected = CustomDamlJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = Some("someLedgerId"),
            participantId = None,
            applicationId = Some("someApplicationId"),
            exp = None,
            admin = false,
            actAs = List("Alice"),
            readAs = List.empty,
          )
        )
        val result = parseCustomToken(serialized)
        result shouldBe Success(expected)
        result.map(_.payload.party) shouldBe Success(Some("Alice"))
      }

      "support standard JWT claims" in {
        val serialized =
          """{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        val expected = StandardJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = None,
            participantId = Some("someParticipantId"),
            applicationId = Some("someUserId"),
            exp = Some(Instant.ofEpochSecond(100)),
            admin = false,
            actAs = List.empty,
            readAs = List.empty,
          )
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
      }

      "have stable default values" in {
        val serialized = "{}"
        val expected = CustomDamlJWTPayload(
          AuthServiceJWTPayload(
            ledgerId = None,
            participantId = None,
            applicationId = None,
            exp = None,
            admin = false,
            actAs = List.empty,
            readAs = List.empty,
          )
        )
        val result = parseCustomToken(serialized)
        result shouldBe Success(expected)
        result.map(_.payload.party) shouldBe Success(None)
      }

    }
  }
}
