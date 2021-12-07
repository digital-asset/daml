// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  private def serializeAndParse(value: AuthServiceJWTPayload): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._

    for {
      serialized <- Try(value.toJson.prettyPrint)
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
    } yield parsed
  }

  /** Parses a [[AuthServiceJWTPayload]] */
  private def parse(serialized: String): Try[AuthServiceJWTPayload] = {
    import AuthServiceJWTCodec.JsonImplicits._

    for {
      json <- Try(serialized.parseJson)
      parsed <- Try(json.convertTo[AuthServiceJWTPayload])
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

      "work for arbitrary values" in forAll(
        Gen.resultOf(AuthServiceJWTPayload),
        minSuccessful(100),
      )(v0 => {
        // FIXME: remove this hack, which is due to not serializing a new JWT token the right way
        val value =
          if (v0.isCustomDamlToken)
            v0
          else
            AuthServiceJWTPayload(
              ledgerId = None,
              participantId = v0.participantId,
              applicationId = Some(v0.applicationId.getOrElse("default-user")),
              exp = v0.exp,
              admin = false,
              actAs = List.empty,
              readAs = List.empty,
              isCustomDamlToken = false,
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
        val expected = AuthServiceJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = Some("someParticipantId"),
          applicationId = Some("someApplicationId"),
          exp = Some(Instant.EPOCH),
          admin = true,
          actAs = List("Alice"),
          readAs = List("Alice", "Bob"),
          isCustomDamlToken = true,
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
        result.map(_.party) shouldBe Success(None)
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
        val expected = AuthServiceJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = Some("someParticipantId"),
          applicationId = Some("someApplicationId"),
          exp = Some(Instant.EPOCH),
          admin = true,
          actAs = List("Alice"),
          readAs = List("Alice", "Bob"),
          isCustomDamlToken = true,
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
        result.map(_.party) shouldBe Success(None)
      }

      "support legacy JSON API format" in {
        val serialized =
          """{
            |  "ledgerId": "someLedgerId",
            |  "applicationId": "someApplicationId",
            |  "party": "Alice"
            |}
          """.stripMargin
        val expected = AuthServiceJWTPayload(
          ledgerId = Some("someLedgerId"),
          participantId = None,
          applicationId = Some("someApplicationId"),
          exp = None,
          admin = false,
          actAs = List("Alice"),
          readAs = List.empty,
          isCustomDamlToken = true,
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
        result.map(_.party) shouldBe Success(Some("Alice"))
      }

      "support standard JWT claims" in {
        val serialized =
          """{
            |  "aud": "someParticipantId",
            |  "sub": "someUserId",
            |  "exp": 100
            |}
          """.stripMargin
        val expected = AuthServiceJWTPayload(
          ledgerId = None,
          participantId = Some("someParticipantId"),
          applicationId = Some("someUserId"),
          exp = Some(Instant.ofEpochSecond(100)),
          admin = false,
          actAs = List.empty,
          readAs = List.empty,
          isCustomDamlToken = false,
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
      }

      "have stable default values" in {
        val serialized = "{}"
        val expected = AuthServiceJWTPayload(
          ledgerId = None,
          participantId = None,
          applicationId = None,
          exp = None,
          admin = false,
          actAs = List.empty,
          readAs = List.empty,
          isCustomDamlToken = true,
        )
        val result = parse(serialized)
        result shouldBe Success(expected)
        result.map(_.party) shouldBe Success(None)
      }

    }
  }
}
