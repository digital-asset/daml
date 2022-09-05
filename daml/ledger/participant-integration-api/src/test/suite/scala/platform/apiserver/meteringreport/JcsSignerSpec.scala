// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.apiserver.meteringreport.JcsSigner.VerificationStatus._
import com.daml.platform.apiserver.meteringreport.MeteringReport.{
  ApplicationReport,
  Check,
  ParticipantReport,
  Request,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.time.temporal.ChronoUnit

class JcsSignerSpec extends AnyWordSpec with Matchers {

  import HmacSha256._

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
      val Right(signed) = JcsSigner.sign(report, testKey)
      JcsSigner.verify(signed, keyLookup) shouldBe Ok
    }

    "ignore existing check" in {
      val Right(expected) = JcsSigner.sign(report.copy(check = None), testKey)
      val Right(actual) =
        JcsSigner.sign(report.copy(check = Some(Check("some", "other"))), testKey)
      actual shouldBe expected
    }

    "fail generation if key is not valid" in {
      val Left(_) = JcsSigner.sign(report, badKey)
    }

    "fail verification if details are changed" in {
      val Right(signed) = JcsSigner.sign(report, testKey)
      val modified = signed.copy(`final` = !signed.`final`)
      val DigestMismatch(_) = JcsSigner.verify(modified, keyLookup)
    }

    "fail verification if JSON is invalid" in {
      val InvalidJson(_) = JcsSigner.verify("""{"bad": json""", keyLookup)
    }

    "fail verification if JSON is not a participant report" in {
      val InvalidJson(_) = JcsSigner.verify("""{"not: "report"}""", keyLookup)
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
      val CheckGeneration(_) = JcsSigner.verify(report.copy(check = Some(check)), _ => Some(badKey))
    }

  }

}
