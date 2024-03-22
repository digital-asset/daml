// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.platform.apiserver.meteringreport.JcsSigner.VerificationStatus.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReport.{
  ApplicationReport,
  Check,
  ParticipantReport,
  Request,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.*

import java.time.Duration
import java.time.temporal.ChronoUnit

import MeteringReport.*

class JcsSignerSpec extends AnyWordSpec with Matchers {

  import HmacSha256.*

  private val testKey: Key = HmacSha256.generateKey("test")
  private val keyLookup = Map(testKey.scheme -> testKey).get _

  private val application = Ref.ApplicationId.assertFromString("a0")
  private val from = Timestamp.now()
  private val to = from.add(Duration.of(1, ChronoUnit.DAYS))
  private val report: ParticipantReport = ParticipantReport(
    participant = Ref.ParticipantId.assertFromString("p0"),
    request = Request(from, Some(to), Some(application)),
    `final` = false,
    applications = Seq(ApplicationReport(application, 272)),
    check = None,
  )

  JcsSigner.getClass.getName should {

    val badKey = Key("bad", Bytes(Array.empty), "bad")

    "sign report" in {
      JcsSigner.sign(report, testKey).map(signed => JcsSigner.verify(signed, keyLookup) shouldBe Ok)
    }

    "verify report json" in {
      JcsSigner.sign(report, testKey).map { signed =>
        val json = signed.toJson.prettyPrint
        JcsSigner.verify(json, keyLookup) shouldBe Ok
      }
    }

    "ignore existing check" in {
      for {
        expected <- JcsSigner.sign(report.copy(check = None), testKey)
        actual <- JcsSigner.sign(report.copy(check = Some(Check("some", "other"))), testKey)
      } yield {
        actual shouldBe expected
      }
    }

    "fail generation if key is not valid" in {
      JcsSigner.sign(report, badKey).isLeft shouldBe true
    }

    "fail verification if details are changed" in {
      for {
        signed <- JcsSigner.sign(report, testKey)
        modified = signed.copy(`final` = !signed.`final`)
      } yield JcsSigner.verify(modified, keyLookup) should matchPattern { case DigestMismatch(_) =>
      }
    }

    "fail verification if JSON is invalid" in {
      JcsSigner.verify("""{"bad": json""", keyLookup) should matchPattern { case InvalidJson(_) => }
    }

    "fail verification if JSON is not a participant report" in {
      JcsSigner.verify("""{"not: "report"}""", keyLookup) should matchPattern {
        case InvalidJson(_) =>
      }
    }

    "fail verification if check sections is missing" in {
      JcsSigner.verify(report.copy(check = None), keyLookup) shouldBe MissingCheckSection
    }

    "fail verification if check scheme is unknown" in {
      val check = Check("unknown", "digest")
      JcsSigner.verify(report.copy(check = Some(check)), keyLookup) shouldBe UnknownScheme(
        check.scheme
      )
    }

    "fail verification check regeneration fails" in {
      val check = Check("unknown", "digest")
      JcsSigner.verify(report.copy(check = Some(check)), _ => Some(badKey)) should matchPattern {
        case CheckGeneration(_) =>
      }
    }

  }

}
