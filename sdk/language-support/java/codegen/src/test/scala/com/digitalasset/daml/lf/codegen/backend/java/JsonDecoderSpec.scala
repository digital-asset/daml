// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java

import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import baz.baz.Baz

final class JsonDecoderSpec extends AnyWordSpec with Matchers {
  "Template" should {
    "be decodable with optional field set" in {
      Baz
        .jsonDecoder()
        .decode(new JsonLfReader("""{"p": "alice", "upgrade": "def"}""")) shouldBe new Baz(
        "alice",
        java.util.Optional.of("def"),
      )
    }
    "be decodable with optional field unset" in {
      Baz.jsonDecoder().decode(new JsonLfReader("""{"p": "alice"}""")) shouldBe new Baz(
        "alice",
        java.util.Optional.empty(),
      )
    }
  }
}
