// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}
import com.daml.timestamps.JavaLf._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class JavaLfTimestampConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Java instant to a Daml-LF timestamp" should {
    "convert to and fro" in {
      // The set of Java instants is much larger than the set of Daml-LF timestamps, so we generate the latter.
      forAll { (timestamp: LfTimestamp) =>
        timestamp.asJava.asLf should be(timestamp)
      }
    }
  }

  private implicit val `Arbitrary LfTimestamp`: Arbitrary[LfTimestamp] = Arbitrary {
    val specials = Seq(0, LfTimestamp.MinValue.micros, LfTimestamp.MaxValue.micros)
    Gen
      .chooseNum(LfTimestamp.MinValue.micros, LfTimestamp.MaxValue.micros, specials: _*)
      .map(LfTimestamp.assertFromLong)
  }
}
