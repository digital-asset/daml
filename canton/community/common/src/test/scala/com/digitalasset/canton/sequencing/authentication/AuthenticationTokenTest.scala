// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import com.digitalasset.canton.BaseTestWordSpec
import com.google.protobuf.ByteString

import java.nio.charset.Charset

class AuthenticationTokenTest extends BaseTestWordSpec {

  "AuthenticationTokens" can {
    "be compared to each other" in {
      val byteString1 = ByteString.copyFrom("abcdabcdabcdabcdabcd", Charset.defaultCharset())
      val byteString1Bis = ByteString.copyFrom("abcdabcdabcdabcdabcd", Charset.defaultCharset())
      val byteString1Longer =
        ByteString.copyFrom("abcdabcdabcdabcdabcdabcd", Charset.defaultCharset())
      val byteString2 = ByteString.copyFrom("defgdefgdefgdefgdefg", Charset.defaultCharset())

      AuthenticationToken(byteString1) shouldBe AuthenticationToken(byteString1)
      AuthenticationToken(byteString1) shouldBe AuthenticationToken(byteString1Bis)
      AuthenticationToken(byteString1) should not be AuthenticationToken(byteString1Longer)
      AuthenticationToken(byteString1) should not be AuthenticationToken(byteString2)
    }
  }
}
