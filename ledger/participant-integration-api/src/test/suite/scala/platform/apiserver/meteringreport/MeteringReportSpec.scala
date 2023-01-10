// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.meteringreport

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.platform.apiserver.meteringreport.MeteringReport._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import java.time.Duration
import java.time.temporal.ChronoUnit
import spray.json.enrichAny
import spray.json._

class MeteringReportSpec extends AsyncWordSpec with Matchers {

  "the metering report" should {

    "serialize and deserialize as JSON" in {

      val application = Ref.ApplicationId.assertFromString("a0")
      val from = Timestamp.now()
      val to = from.add(Duration.of(1, ChronoUnit.DAYS))

      val expected = ParticipantReport(
        participant = Ref.ParticipantId.assertFromString("p0"),
        request = Request(from, Some(to), Some(application)),
        `final` = false,
        applications = Seq(ApplicationReport(application, 272)),
        check = Some(Check("community", "digest0")),
      )

      val json = expected.toJson.prettyPrint
      val actual = json.parseJson.convertTo[ParticipantReport]
      actual shouldBe expected

    }

  }

}
