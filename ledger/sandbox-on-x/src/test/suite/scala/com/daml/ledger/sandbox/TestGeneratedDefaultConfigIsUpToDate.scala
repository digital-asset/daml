// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.scalatest.{AppendedClues, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestGeneratedDefaultConfigIsUpToDate
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with AppendedClues {

  private val clue =
    "Generated default config is not up-to-date. \nPlease run: bazel run //ledger/sandbox-on-x:update-generated-sources"

  it should "generated default config should be up-to-date" in {
    val actual = readFromResource("generated-default.conf")
    val expected = DefaultConfigGenApp.genText().replaceAll("\\r", "")
    actual.value shouldBe expected withClue (clue)
  }

  private def readFromResource(resourceName: String): Option[String] = {
    val streamO = Option(getClass.getClassLoader.getResourceAsStream(resourceName))
    streamO.map(stream => IOUtils.toString(stream, StandardCharsets.UTF_8))
  }

}
