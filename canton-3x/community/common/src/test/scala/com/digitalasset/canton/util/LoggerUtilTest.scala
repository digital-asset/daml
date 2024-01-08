// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTestWordSpec

class LoggerUtilTest extends BaseTestWordSpec {

  lazy val testString =
    """I hate bananas
  |I actually do like em
  |But I prefer bockwurst 
  |""".stripMargin

  "string truncation" should {
    "not truncate when staying within limits" in {
      LoggerUtil.truncateString(4, 100)(testString) shouldBe testString
    }
    "truncate when seeing too many lines" in {
      LoggerUtil.truncateString(maxLines = 1, maxSize = 100)(
        testString
      ) shouldBe ("I hate bananas\n ...")
    }
    "truncate when seeing too many characters" in {
      LoggerUtil.truncateString(maxLines = 40, maxSize = 25)(
        testString
      ) shouldBe "I hate bananas\nI actually ..."
    }
  }

}
