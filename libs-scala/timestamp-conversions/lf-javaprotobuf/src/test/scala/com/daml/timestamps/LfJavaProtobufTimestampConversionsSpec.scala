// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}
import com.daml.timestamps.LfJavaProtobuf._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class LfJavaProtobufTimestampConversionsSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  "converting a Daml-LF timestamp to a Java Protocol Buffers timestamp" should {
    "convert to and fro" in {
      // The set of Protocol Buffers timestamps is much larger than the set of Daml-LF timestamps, so we generate the latter.
      forAll { (timestamp: LfTimestamp) =>
        timestamp.asJavaProto.asLf should be(timestamp)
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
