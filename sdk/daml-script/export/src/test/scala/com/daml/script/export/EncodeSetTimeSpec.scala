// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.lf.data.Time.Timestamp
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeSetTimeSpec extends AnyFreeSpec with Matchers {
  import Encode._
  "encodeSetTime" in {
    encodeSetTime(Timestamp.assertFromString("1990-11-09T04:30:23.123456Z")).render(80) shouldBe
      """setTime (DA.Time.time (DA.Date.date 1990 DA.Date.Nov 9) 4 30 23)"""
  }
}
